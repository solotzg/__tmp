// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

/*
mysql -h 0.0.0.0 -P 12354 -u root < example3-s3/tidb.sql
./setup-demo.py --cmd sink_task --sink_task_desc="etl3.3.demo.t3" --sink_task_flink_schema_path ./example3-s3/flink.sql.template --cdc_sink_s3
mysql -h 0.0.0.0 -P 12354 -u root -e "insert into demo.t3 (select max(a)+1,max(a)+1 from demo.t3)"
cd ticdc/storage-consumer
go build -o ./bin/
./bin/storage-consumer -upstream-uri="s3://etl3-sink-3/ticdc-sink?protocol=canal-json&endpoint=http://10.2.12.124:12350&access-Key=minioadmin&secret-Access-Key=minioadmin&enable-tidb-extension=true" -downstream-uri="blackhole://" -flush-interval=2000ms -log-level=debug
*/

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	tidb "database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink"
	"github.com/pingcap/tiflow/cdc/sink/tablesink"
	sinkutil "github.com/pingcap/tiflow/cdc/sink/util"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/container/sortmap"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/pingcap/tiflow/pkg/quotes"
	psink "github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/canal"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/spanz"
	putil "github.com/pingcap/tiflow/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var (
	upstreamURIStr  string
	upstreamURI     *url.URL
	configFile      string
	logFile         string
	logLevel        string
	flushInterval   time.Duration
	fileIndexWidth  int
	enableProfiling bool
	timezone        string
	tidbAddr        string
	tidbTestSQL     string
	runTestSQL      bool
	testStr         string
	etlTables       map[string][]model.Column
)

const (
	defaultChangefeedName         = "storage-consumer"
	defaultFlushWaitDuration      = 200 * time.Millisecond
	fakePartitionNumForSchemaFile = -1
)

func init() {
	var tables string
	flag.StringVar(&upstreamURIStr, "upstream-uri", "", "storage uri")
	flag.StringVar(&configFile, "config", "", "changefeed configuration file")
	flag.StringVar(&logFile, "log-file", "", "log file path")
	flag.StringVar(&logLevel, "log-level", "info", "log level")
	flag.DurationVar(&flushInterval, "flush-interval", 10*time.Second, "flush interval")
	flag.IntVar(&fileIndexWidth, "file-index-width",
		config.DefaultFileIndexWidth, "file index width")
	flag.BoolVar(&enableProfiling, "enable-profiling", false, "whether to enable profiling")
	flag.StringVar(&timezone, "tz", "System", "Specify time zone of storage consumer")
	flag.StringVar(&tidbAddr, "tidb-addr", "127.0.0.1:12354", "Specify tidb address `host/ip`:`port`")
	flag.StringVar(&tidbTestSQL, "test-sql", "", "sql: select ? from ? where ?")
	flag.StringVar(&testStr, "test-str", "", "")
	flag.StringVar(&tables, "etl-tables", "", "")

	flag.Parse()

	if len(tidbTestSQL) > 0 {
		runTestSQL = true
		return
	}

	etlTables = make(map[string][]model.Column)
	for _, tableKey := range strings.Split(tables, ",") {
		x := strings.Split(tableKey, ".")
		if len(x) != 2 {
			panic("")
		}
		schema := x[0]
		table := x[1]
		etlTables[quotes.QuoteSchema(schema, table)] = make([]model.Column, 0)
	}

	err := logutil.InitLogger(&logutil.Config{
		Level: logLevel,
		File:  logFile,
	})
	if err != nil {
		log.Error("init logger failed", zap.Error(err))
		os.Exit(1)
	}

	uri, err := url.Parse(upstreamURIStr)
	if err != nil {
		log.Error("invalid upstream-uri", zap.Error(err))
		os.Exit(1)
	}
	upstreamURI = uri
	scheme := strings.ToLower(upstreamURI.Scheme)
	if !psink.IsStorageScheme(scheme) {
		log.Error("invalid storage scheme, the scheme of upstream-uri must be file/s3/azblob/gcs")
		os.Exit(1)
	}
}

// fileIndexRange defines a range of files. eg. CDC000002.csv ~ CDC000005.csv
type fileIndexRange struct {
	start uint64
	end   uint64
}

type TxnSinkMap map[uint64][]*model.RowChangedEvent
type TableDefMap map[string]map[uint64]*cloudstorage.TableDefinition

type consumer struct {
	replicationCfg  *config.ReplicaConfig
	codecCfg        *common.Config
	externalStorage storage.ExternalStorage
	fileExtension   string
	// tableDMLIdxMap maintains a map of <dmlPathKey, max file index>
	tableDMLIdxMap map[cloudstorage.DmlPathKey]uint64
	// tableTsMap maintains a map of <TableID, max commit ts>
	tableTsMap map[model.TableID]model.ResolvedTs
	// tableDefMap maintains a map of <`schema`.`table`, tableDef slice sorted by TableVersion>
	tableDefMap map[string]map[uint64]*cloudstorage.TableDefinition
	// tableSinkMap maintains a map of <TableID, TableSink>
	tableSinkMap     map[model.TableID]tablesink.TableSink
	txnSinkMap       TxnSinkMap
	tableIDGenerator *fakeTableIDGenerator
	errCh            chan error
	db               *tidb.DB
	CheckpointTs     uint64
}

func newConsumer(ctx context.Context) (*consumer, error) {
	_, err := putil.GetTimezone(timezone)
	if err != nil {
		return nil, errors.Annotate(err, "can not load timezone")
	}
	serverCfg := config.GetGlobalServerConfig().Clone()
	serverCfg.TZ = timezone
	config.StoreGlobalServerConfig(serverCfg)
	replicaConfig := config.GetDefaultReplicaConfig()
	if len(configFile) > 0 {
		err := util.StrictDecodeFile(configFile, "storage consumer", replicaConfig)
		if err != nil {
			log.Error("failed to decode config file", zap.Error(err))
			return nil, err
		}
	}

	err = replicaConfig.ValidateAndAdjust(upstreamURI)
	if err != nil {
		log.Error("failed to validate replica config", zap.Error(err))
		return nil, err
	}

	switch putil.GetOrZero(replicaConfig.Sink.Protocol) {
	case config.ProtocolCsv.String():
	case config.ProtocolCanalJSON.String():
	default:
		return nil, fmt.Errorf(
			"data encoded in protocol %s is not supported yet",
			putil.GetOrZero(replicaConfig.Sink.Protocol),
		)
	}

	protocol, err := config.ParseSinkProtocolFromString(putil.GetOrZero(replicaConfig.Sink.Protocol))
	if err != nil {
		return nil, err
	}

	codecConfig := common.NewConfig(protocol)
	err = codecConfig.Apply(upstreamURI, replicaConfig)
	if err != nil {
		return nil, err
	}

	extension := sinkutil.GetFileExtension(protocol)

	storage, err := putil.GetExternalStorageFromURI(ctx, upstreamURIStr)
	if err != nil {
		log.Error("failed to create external storage", zap.Error(err))
		return nil, err
	}

	errCh := make(chan error, 1)

	db, err := newMySQLConn(tidbAddr)
	if err != nil {
		return nil, err
	}

	for tableKey, _ := range etlTables {
		res, err := getJSON(db, "desc "+tableKey)
		if err != nil {
			return nil, err
		}
		columns := make([]model.Column, len(res))
		for i, r := range res {
			columns[i].Name = r[0]
			if r[3] == "PRI" {
				columns[i].Flag.SetIsPrimaryKey()
			}
		}
		etlTables[tableKey] = columns
	}

	return &consumer{
		replicationCfg:  replicaConfig,
		codecCfg:        codecConfig,
		externalStorage: storage,
		fileExtension:   extension,
		errCh:           errCh,
		tableDMLIdxMap:  make(map[cloudstorage.DmlPathKey]uint64),
		tableTsMap:      make(map[model.TableID]model.ResolvedTs),
		tableDefMap:     make(map[string]map[uint64]*cloudstorage.TableDefinition),
		tableSinkMap:    make(map[model.TableID]tablesink.TableSink),
		tableIDGenerator: &fakeTableIDGenerator{
			tableIDs: make(map[string]int64),
		},
		txnSinkMap: make(TxnSinkMap),
		db:         db,
	}, nil
}

// map1 - map2
func diffDMLMaps(
	map1, map2 map[cloudstorage.DmlPathKey]uint64,
) map[cloudstorage.DmlPathKey]fileIndexRange {
	resMap := make(map[cloudstorage.DmlPathKey]fileIndexRange)
	for k, v := range map1 {
		if _, ok := map2[k]; !ok {
			resMap[k] = fileIndexRange{
				start: 1,
				end:   v,
			}
		} else if v > map2[k] {
			resMap[k] = fileIndexRange{
				start: map2[k] + 1,
				end:   v,
			}
		}
	}

	return resMap
}

// getNewFiles returns newly created dml files in specific ranges
func (c *consumer) getNewFiles(
	ctx context.Context,
) (map[cloudstorage.DmlPathKey]fileIndexRange, error) {
	tableDMLMap := make(map[cloudstorage.DmlPathKey]fileIndexRange)
	opt := &storage.WalkOption{SubDir: ""}

	origDMLIdxMap := make(map[cloudstorage.DmlPathKey]uint64, len(c.tableDMLIdxMap))
	for k, v := range c.tableDMLIdxMap {
		origDMLIdxMap[k] = v
	}

	err := c.externalStorage.WalkDir(ctx, opt, func(path string, size int64) error {
		if cloudstorage.IsSchemaFile(path) {
			err := c.parseSchemaFilePath(ctx, path)
			if err != nil {
				log.Error("failed to parse schema file path", zap.Error(err))
				// skip handling this file
				return nil
			}
		} else if strings.HasSuffix(path, c.fileExtension) {
			err := c.parseDMLFilePath(ctx, path)
			if err != nil {
				log.Error("failed to parse dml file path", zap.Error(err))
				// skip handling this file
				return nil
			}
		} else {
			log.Debug("ignore handling file", zap.String("path", path))
		}
		return nil
	})
	if err != nil {
		return tableDMLMap, err
	}

	tableDMLMap = diffDMLMaps(c.tableDMLIdxMap, origDMLIdxMap)
	return tableDMLMap, err
}

type metadata struct {
	CheckpointTs uint64 `json:"checkpoint-ts"`
}

func (c *consumer) getCheckpointTs(
	ctx context.Context,
) (uint64, error) {
	origDMLIdxMap := make(map[cloudstorage.DmlPathKey]uint64, len(c.tableDMLIdxMap))
	for k, v := range c.tableDMLIdxMap {
		origDMLIdxMap[k] = v
	}

	var meta metadata
	metadataContent, err := c.externalStorage.ReadFile(ctx, "metadata")
	if err != nil {
		return 0, err
	}
	err = json.Unmarshal(metadataContent, &meta)
	if err != nil {
		return 0, err
	}

	return meta.CheckpointTs, nil
}

var _ dmlsink.EventSink[*model.RowChangedEvent] = (*IMVSink)(nil)

type IMVSink struct {
	joinKeys   []string
	table1     string
	table2     string
	txnSinkMap TxnSinkMap
}

func newMySQLConn(addr string) (*tidb.DB, error) {
	return tidb.Open("mysql", fmt.Sprintf("root:@tcp(%s)/mysql", addr))
}

func newIMVSink(joinKeys []string, table1, table2 string, txnSinkMap TxnSinkMap, addr string) (*IMVSink, error) {
	return &IMVSink{
		joinKeys:   joinKeys,
		table1:     table1,
		table2:     table2,
		txnSinkMap: txnSinkMap,
	}, nil
}

func (v *IMVSink) Close() {
}

func getJSONAtTs(db *tidb.DB, sqlString string, ts uint64) ([][]string, error) {
	sqlString = fmt.Sprintf("set tidb_snapshot=%d; %s", ts, sqlString)
	return getJSON(db, sqlString)
}

func getJSON(db *tidb.DB, sqlString string) ([][]string, error) {
	rows, err := db.Query(sqlString)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	count := len(columns)
	tableData := make([][]string, 0)
	valuePtrs := make([]interface{}, count)
	for rows.Next() {
		tmp := make([]interface{}, count)
		values := make([]string, count)
		for i := 0; i < count; i++ {
			valuePtrs[i] = &tmp[i]
		}
		rows.Scan(valuePtrs...)
		for i := range columns {
			var v string
			val := tmp[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else if val != nil {
				panic("")
			}
			values[i] = v
		}
		tableData = append(tableData, values)
	}
	return tableData, nil
}

func into_condition(table string, keys []string, values []string) string {
	condition := []string{}
	for i := range keys {
		condition = append(condition, fmt.Sprintf("%s.%s = '%s'", table, keys[i], values[i]))
	}
	return strings.Join(condition, " and ")
}

// WriteEvents log the events.
func (s *IMVSink) WriteEvents(rows ...*dmlsink.CallbackableEvent[*model.RowChangedEvent]) error {
	for _, row := range rows {
		if v, ok := s.txnSinkMap[row.Event.CommitTs]; !ok {
			s.txnSinkMap[row.Event.CommitTs] = []*model.RowChangedEvent{row.Event}
		} else {
			s.txnSinkMap[row.Event.CommitTs] = append(v, row.Event)
		}
	}

	return nil
}

// Dead returns a checker.
func (s *IMVSink) Dead() <-chan struct{} {
	return make(chan struct{})
}

// emitDMLEvents decodes RowChangedEvents from file content and emit them.
func (c *consumer) emitDMLEvents(
	ctx context.Context, tableID int64,
	tableDetail cloudstorage.TableDefinition,
	pathKey cloudstorage.DmlPathKey,
	content []byte,
) error {
	var (
		decoder codec.RowEventDecoder
		err     error
	)

	if c.codecCfg.Protocol != config.ProtocolCanalJSON {
		return errors.Trace(errors.Errorf("expect %s but got %s", config.ProtocolCanalJSON.String(), c.codecCfg.Protocol.String()))
	} else {
		// Always enable tidb extension for canal-json protocol
		// because we need to get the commit ts from the extension field.
		c.codecCfg.EnableTiDBExtension = true
		decoder, err = canal.NewBatchDecoder(ctx, c.codecCfg, nil)
		if err != nil {
			return errors.Trace(err)
		}
		err := decoder.AddKeyValue(nil, content)
		if err != nil {
			return errors.Trace(err)
		}
	}

	cnt := 0
	filteredCnt := 0
	for {
		tp, hasNext, err := decoder.HasNext()
		if err != nil {
			log.Error("failed to decode message", zap.Error(err))
			return err
		}
		if !hasNext {
			break
		}
		cnt++

		if tp == model.MessageTypeRow {
			row, err := decoder.NextRowChangedEvent()
			if err != nil {
				log.Error("failed to get next row changed event", zap.Error(err))
				return errors.Trace(err)
			}

			if _, ok := c.tableSinkMap[tableID]; !ok {
				imv, err := newIMVSink(nil, "", "", c.txnSinkMap, tidbAddr)
				if err != nil {
					log.Error("failed to create IMV", zap.Error(err))
					return errors.Trace(err)
				}

				var rowSink dmlsink.EventSink[*model.RowChangedEvent] = imv
				c.tableSinkMap[tableID] = tablesink.New(model.DefaultChangeFeedID(defaultChangefeedName),
					spanz.TableIDToComparableSpan(tableID),
					row.CommitTs, rowSink,
					&dmlsink.RowChangeEventAppender{},
					prometheus.NewCounter(prometheus.CounterOpts{}))
			}

			_, ok := c.tableTsMap[tableID]
			if !ok || row.CommitTs > c.tableTsMap[tableID].Ts {
				c.tableTsMap[tableID] = model.ResolvedTs{
					Mode:    model.BatchResolvedMode,
					Ts:      row.CommitTs,
					BatchID: 1,
				}
			} else if row.CommitTs == c.tableTsMap[tableID].Ts {
				c.tableTsMap[tableID] = c.tableTsMap[tableID].AdvanceBatch()
			} else {
				log.Warn("row changed event commit ts fallback, ignore",
					zap.Uint64("commitTs", row.CommitTs),
					zap.Any("tableMaxCommitTs", c.tableTsMap[tableID]),
					zap.Any("row", row),
				)
				continue
			}
			row.Table.TableID = tableID
			c.tableSinkMap[tableID].AppendRowChangedEvents(row)
			filteredCnt++
		}
	}
	log.Info("decode success", zap.String("schema", pathKey.Schema),
		zap.String("table", pathKey.Table),
		zap.Uint64("version", pathKey.TableVersion),
		zap.Int("decodeRowsCnt", cnt),
		zap.Int("filteredRowsCnt", filteredCnt))

	return err
}

func (c *consumer) isSafeCheckpointTs(ts uint64, tableID int64, allInCache bool) bool {
	// if all data of table has been handled (in cache), ts is safe if in range (, c.CheckpointTs]
	// else ts is safe of in range (, c.tableTsMap[tableID].Ts)
	if allInCache {
		return ts <= c.CheckpointTs
	} else {
		return ts < c.tableTsMap[tableID].Ts
	}
}

// "select * from t1, t2 where t1.pk = ? and t2. "
func calc(events []*model.RowChangedEvent, joinCondition string, db *tidb.DB) map[string][][]string {
	col_desc := make([]*model.Column, 0)
	pk_size := 0
	tableSchemas := make([]string, 0)
	for key, cols := range etlTables {
		for i := range cols {
			col_desc = append(col_desc, &cols[i])
			if cols[i].Flag.IsPrimaryKey() {
				pk_size += 1
			}
		}
		tableSchemas = append(tableSchemas, key)
	}
	newEtlPKMap := make(map[string][]string)
	oldEtlPKMap := make(map[string][]string)
	for _, event := range events {
		if !event.IsDelete() {
			pk := []string{}
			pkVal := []string{}
			for _, col := range event.Columns {
				if col == nil {
					continue
				}
				if col.Flag.IsPrimaryKey() {
					pk = append(pk, col.Name)
					pkVal = append(pkVal, model.ColumnValueString(col.Value))
				}
			}
			schema := quotes.QuoteSchema(event.Table.Schema, event.Table.Table)
			c := into_condition(schema, pk, pkVal)
			if len(joinCondition) > 0 {
				c += " and " + joinCondition
			}
			sql := fmt.Sprintf("select * from %s where %s", strings.Join(tableSchemas, ","), c)
			res_rows, err := getJSONAtTs(db, sql, event.CommitTs)
			if err != nil {
				panic(err.Error())
			}
			for _, row := range res_rows {
				k := make([]string, 0, pk_size)
				for c := range col_desc {
					if col_desc[c].Flag.IsPrimaryKey() {
						k = append(k, row[c])
					}
				}
				newEtlPKMap[strings.Join(k, "\t")] = row
			}
		}

		if !event.IsInsert() {
			pk := []string{}
			pkVal := []string{}
			for _, col := range event.PreColumns {
				if col == nil {
					continue
				}
				if col.Flag.IsPrimaryKey() {
					pk = append(pk, col.Name)
					pkVal = append(pkVal, model.ColumnValueString(col.Value))
				}
			}
			schema := quotes.QuoteSchema(event.Table.Schema, event.Table.Table)
			c := into_condition(schema, pk, pkVal)
			if len(joinCondition) > 0 {
				c += " and " + joinCondition
			}
			sql := fmt.Sprintf("select * from %s where %s", strings.Join(tableSchemas, ","), c)
			res_rows, err := getJSONAtTs(db, sql, event.CommitTs-1)
			if err != nil {
				panic(err.Error())
			}
			for _, row := range res_rows {
				k := make([]string, 0, pk_size)
				for c := range col_desc {
					if col_desc[c].Flag.IsPrimaryKey() {
						k = append(k, row[c])
					}
				}
				oldEtlPKMap[strings.Join(k, "\t")] = row
			}
		}
	}

	resEtlPKMap := make(map[string][][]string)
	for k, v := range oldEtlPKMap {
		resEtlPKMap[k] = [][]string{v, nil}
	}
	for k, v := range newEtlPKMap {
		if old, ok := resEtlPKMap[k]; ok {
			old[1] = v
		} else {
			resEtlPKMap[k] = [][]string{nil, v}
		}
	}

	return resEtlPKMap
}

func (c *consumer) updateTableResolvedTs() error {
	globalCheckpoint := model.NewResolvedTs(c.CheckpointTs)
	for _, m := range c.tableSinkMap {
		err := m.UpdateResolvedTs(globalCheckpoint)
		if err != nil {
			return err
		}
	}

	if len(c.txnSinkMap) != 0 {
		for _, m := range sortmap.Sort(c.txnSinkMap) {
			for k, v := range calc(m.Value, testStr, c.db) {
				log.Debug("Handle etl row", zap.Uint64("ts", m.Key), zap.String("key", k), zap.Any("value", v))
			}
		}
		c.txnSinkMap = make(TxnSinkMap)
	}

	return nil
}

func (c *consumer) syncExecDMLEvents(
	ctx context.Context,
	tableDef cloudstorage.TableDefinition,
	key cloudstorage.DmlPathKey,
	fileIdx uint64,
) error {
	filePath := key.GenerateDMLFilePath(fileIdx, c.fileExtension, fileIndexWidth)
	log.Debug("read from dml file path", zap.String("path", filePath))
	content, err := c.externalStorage.ReadFile(ctx, filePath)
	if err != nil {
		return errors.Trace(err)
	}
	tableID := c.tableIDGenerator.generateFakeTableID(
		key.Schema, key.Table, key.PartitionNum)

	err = c.emitDMLEvents(ctx, tableID, tableDef, key, content)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (c *consumer) parseDMLFilePath(_ context.Context, path string) error {
	var dmlkey cloudstorage.DmlPathKey
	fileIdx, err := dmlkey.ParseDMLFilePath(
		putil.GetOrZero(c.replicationCfg.Sink.DateSeparator),
		path,
	)
	if err != nil {
		return errors.Trace(err)
	}

	if _, ok := c.tableDMLIdxMap[dmlkey]; !ok || fileIdx >= c.tableDMLIdxMap[dmlkey] {
		c.tableDMLIdxMap[dmlkey] = fileIdx
	}
	return nil
}

func (c *consumer) parseSchemaFilePath(ctx context.Context, path string) error {
	var schemaKey cloudstorage.SchemaPathKey
	checksumInFile, err := schemaKey.ParseSchemaFilePath(path)
	if err != nil {
		return errors.Trace(err)
	}
	key := schemaKey.GetKey()
	if tableDefs, ok := c.tableDefMap[key]; ok {
		if _, ok := tableDefs[schemaKey.TableVersion]; ok {
			// Skip if tableDef already exists.
			return nil
		}
	} else {
		c.tableDefMap[key] = make(map[uint64]*cloudstorage.TableDefinition)
	}

	// Read tableDef from schema file and check checksum.
	var tableDef cloudstorage.TableDefinition
	schemaContent, err := c.externalStorage.ReadFile(ctx, path)
	if err != nil {
		return errors.Trace(err)
	}
	err = json.Unmarshal(schemaContent, &tableDef)
	if err != nil {
		return errors.Trace(err)
	}
	checksumInMem, err := tableDef.Sum32(nil)
	if err != nil {
		return errors.Trace(err)
	}
	if checksumInMem != checksumInFile || schemaKey.TableVersion != tableDef.TableVersion {
		log.Panic("checksum mismatch",
			zap.Uint32("checksumInMem", checksumInMem),
			zap.Uint32("checksumInFile", checksumInFile),
			zap.Uint64("tableversionInMem", schemaKey.TableVersion),
			zap.Uint64("tableversionInFile", tableDef.TableVersion),
			zap.String("path", path))
	}

	// Update tableDefMap.
	c.tableDefMap[key][tableDef.TableVersion] = &tableDef

	// Fake a dml key for schema.json file, which is useful for putting DDL
	// in front of the DML files when sorting.
	// e.g, for the partitioned table:
	//
	// test/test1/439972354120482843/schema.json					(partitionNum = -1)
	// test/test1/439972354120482843/55/2023-03-09/CDC000001.csv	(partitionNum = 55)
	// test/test1/439972354120482843/66/2023-03-09/CDC000001.csv	(partitionNum = 66)
	//
	// and for the non-partitioned table:
	// test/test2/439972354120482843/schema.json				(partitionNum = -1)
	// test/test2/439972354120482843/2023-03-09/CDC000001.csv	(partitionNum = 0)
	// test/test2/439972354120482843/2023-03-09/CDC000002.csv	(partitionNum = 0)
	//
	// the DDL event recorded in schema.json should be executed first, then the DML events
	// in csv files can be executed.
	dmlkey := cloudstorage.DmlPathKey{
		SchemaPathKey: schemaKey,
		PartitionNum:  fakePartitionNumForSchemaFile,
		Date:          "",
	}
	if _, ok := c.tableDMLIdxMap[dmlkey]; !ok {
		c.tableDMLIdxMap[dmlkey] = 0
	} else {
		// duplicate table schema file found, this should not happen.
		log.Panic("duplicate schema file found",
			zap.String("path", path), zap.Any("tableDef", tableDef),
			zap.Any("schemaKey", schemaKey), zap.Any("dmlkey", dmlkey))
	}
	return nil
}

func (c *consumer) mustGetTableDef(key cloudstorage.SchemaPathKey) cloudstorage.TableDefinition {
	var tableDef *cloudstorage.TableDefinition
	if tableDefs, ok := c.tableDefMap[key.GetKey()]; ok {
		tableDef = tableDefs[key.TableVersion]
	}
	if tableDef == nil {
		log.Panic("tableDef not found", zap.Any("key", key), zap.Any("tableDefMap", c.tableDefMap))
	}
	return *tableDef
}

func (c *consumer) handleNewFiles(
	ctx context.Context,
	dmlFileMap map[cloudstorage.DmlPathKey]fileIndexRange,
) error {
	keys := make([]cloudstorage.DmlPathKey, 0, len(dmlFileMap))
	for k := range dmlFileMap {
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		log.Info("no new dml files found since last round")
		return nil
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].TableVersion != keys[j].TableVersion {
			return keys[i].TableVersion < keys[j].TableVersion
		}
		if keys[i].PartitionNum != keys[j].PartitionNum {
			return keys[i].PartitionNum < keys[j].PartitionNum
		}
		if keys[i].Date != keys[j].Date {
			return keys[i].Date < keys[j].Date
		}
		if keys[i].Schema != keys[j].Schema {
			return keys[i].Schema < keys[j].Schema
		}
		return keys[i].Table < keys[j].Table
	})

	for _, key := range keys {
		tableDef := c.mustGetTableDef(key.SchemaPathKey)
		// // if the key is a fake dml path key which is mainly used for
		// // sorting schema.json file before the dml files, then execute the ddl query.
		// if key.PartitionNum == fakePartitionNumForSchemaFile &&
		// 	len(key.Date) == 0 && len(tableDef.Query) > 0 {
		// 	ddlEvent, err := tableDef.ToDDLEvent()
		// 	if err != nil {
		// 		return err
		// 	}
		// 	if err := c.ddlSink.WriteDDLEvent(ctx, ddlEvent); err != nil {
		// 		return errors.Trace(err)
		// 	}
		// 	// TODO: need to cleanup tableDefMap in the future.
		// 	log.Info("execute ddl event successfully", zap.String("query", tableDef.Query))
		// 	continue
		// }

		fileRange := dmlFileMap[key]
		for i := fileRange.start; i <= fileRange.end; i++ {
			if err := c.syncExecDMLEvents(ctx, tableDef, key, i); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *consumer) run(ctx context.Context) error {
	ticker := time.NewTicker(flushInterval)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-c.errCh:
			return err
		case <-ticker.C:
		}

		checkpointTs, err := c.getCheckpointTs(ctx)
		if err != nil {
			return errors.Trace(err)
		}

		c.CheckpointTs = checkpointTs

		log.Debug("run one round of consumer", zap.Uint64("checkpoint-ts", checkpointTs))

		dmlFileMap, err := c.getNewFiles(ctx)

		if err != nil {
			return errors.Trace(err)
		}

		err = c.handleNewFiles(ctx, dmlFileMap)
		if err != nil {
			return errors.Trace(err)
		}

		err = c.updateTableResolvedTs()
		if err != nil {
			return errors.Trace(err)
		}

	}
}

// copied from kafka-consumer
type fakeTableIDGenerator struct {
	tableIDs       map[string]int64
	currentTableID int64
	mu             sync.Mutex
}

func (g *fakeTableIDGenerator) generateFakeTableID(schema, table string, partition int64) int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	key := quotes.QuoteSchema(schema, table)
	if partition != 0 {
		key = fmt.Sprintf("%s.`%d`", key, partition)
	}
	if tableID, ok := g.tableIDs[key]; ok {
		return tableID
	}
	g.currentTableID++
	g.tableIDs[key] = g.currentTableID
	return g.currentTableID
}

func testSQL() {
	db, err := newMySQLConn(tidbAddr)
	if err != nil {
		panic(err.Error())
	}
	r, err := getJSON(db, tidbTestSQL)
	if err != nil {
		panic(err.Error())
	}
	fmt.Print(r)
}

func main() {
	if runTestSQL {
		testSQL()
		return
	}

	var consumer *consumer
	var err error

	if enableProfiling {
		go func() {
			server := &http.Server{
				Addr:              ":6060",
				ReadHeaderTimeout: 5 * time.Second,
			}

			if err := server.ListenAndServe(); err != nil {
				log.Fatal("http pprof", zap.Error(err))
			}
		}()
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	deferFunc := func() int {
		stop()
		if consumer != nil {
			for _, v := range consumer.tableSinkMap {
				v.Close()
			}
		}
		if err != nil && err != context.Canceled {
			return 1
		}
		return 0
	}

	consumer, err = newConsumer(ctx)
	if err != nil {
		log.Error("failed to create storage consumer", zap.Error(err))
		goto EXIT
	}

	if err = consumer.run(ctx); err != nil {
		log.Error("error occurred while running consumer", zap.Error(err))
	}

EXIT:
	os.Exit(deferFunc())
}
