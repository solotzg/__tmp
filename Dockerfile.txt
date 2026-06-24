FROM alpine:3.20

RUN apk add --no-cache wget tar && \
    addgroup -g 1000 -S appgroup && \
    adduser -u 1000 -S appuser -G appgroup

WORKDIR /app

COPY NOTICE.txt .

ARG SING_BOX_VERSION=1.12.24
RUN apk add --no-cache wget tar && \
    wget https://github.com/SagerNet/sing-box/releases/download/v${SING_BOX_VERSION}/sing-box-${SING_BOX_VERSION}-linux-amd64.tar.gz && \
    tar -zxvf sing-box-${SING_BOX_VERSION}-linux-amd64.tar.gz && \
    mv sing-box-${SING_BOX_VERSION}-linux-amd64/sing-box ./ && \
    rm -rf sing-box-${SING_BOX_VERSION}-linux-amd64* && \
    apk del wget tar

COPY config.json .

USER appuser

EXPOSE 8080

CMD ["./sing-box", "run", "-c", "config.json"]
