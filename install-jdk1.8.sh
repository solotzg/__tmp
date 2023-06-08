set -ex

yum install java-1.8.0-openjdk-devel -y
yum install java-1.8.0-openjdk -y
javac_path=$(find /usr/lib/jvm/java-1.8.0* -name javac)
JAVA_HOME=$(
    cd $(dirname ${javac_path})/../
    pwd
)
echo "JAVA_HOME:${JAVA_HOME}"
