#!/bin/bash
set -ax
MAIN_CLASS="com.test.hbase.job.HbaseCronJob"
SPRING_SERVER_HOME=$(cd $(dirname $0);cd ..; pwd)
BIN_PATH=$(cd $(dirname $0); pwd)
SERVICE_JAR_PATH=${SPRING_SERVER_HOME}/lib
SERVICE_CONF_PATH=${SPRING_SERVER_HOME}/conf
SERVICE_LOG_PATH=${SPRING_SERVER_HOME}/logs
SERVICE_LOG_FILE=${SERVICE_LOG_PATH}/service.log
SHELL_FILE=$0

HBASE_LIB=/usr/lib/hbase/


appName=$(basename  $(realpath $SPRING_SERVER_HOME))
appName=${appName}

SERVICE_JVM_OPS="-XX:+UseG1GC -XX:+HeapDumpOnOutOfMemoryError -Xms512M -Xmx1G"
SERVICE_JAVA_OPS="-Dspring.profiles.active=pro -Dfile.encoding=UTF-8 -Duser.timezone=Asia/Shanghai"
echo  $SERVICE_JAVA_OPS
JAVA="java -server"


if [ -d $SERVICE_LOG_PATH ];then
echo "find log path: ${SERVICE_LOG_PATH}"
else
echo "create log path: ${SERVICE_LOG_PATH}"
mkdir -p ${SERVICE_LOG_PATH}
fi

if [ -z $appName ]
then
    echo "Please check that this script and your jar-package is in the same directory!"
    exit 1
fi

killForceFlag=$2

function start()
{
    count=`ps -ef |grep java|grep $appName|wc -l`
    if [ $count != 0 ];then
        echo "Maybe $appName is running, please check it..."
    else
        echo "The $appName is starting..."

        echo "commend line ......."
        nohup  ${JAVA} ${SERVICE_JVM_OPS}  ${SERVICE_JAVA_OPS} \
         -cp  ${SERVICE_JAR_PATH}/*:${HBASE_LIB}/*:${HBASE_LIB}/lib/*:/usr/lib/kyuubi/beeline-jars/hadoop-client-runtime-3.3.4.jar:${SERVICE_CONF_PATH}:  \
         ${MAIN_CLASS}  > ${SERVICE_LOG_FILE}  2>&1 &
    fi
}

function stop()
{
    appId=`ps -ef |grep java|grep $appName|awk '{print $2}'`
    if [ -z $appId ]
    then
        echo "Maybe $appName not running, please check it..."
    else
        echo -n "The $appName is stopping..."
        if [ "$killForceFlag" == "-f" ]
        then
            echo "by force"
            kill -9 $appId
        else
            echo
            kill $appId
        fi
    fi
}

function status()
{
    appId=`ps -ef |grep java|grep $appName|awk '{print $2}'`
    if [ -z $appId ]
    then
        echo -e "\033[31m Not running \033[0m"
    else
        echo -e "\033[32m Running [$appId] \033[0m"
    fi
}

function restart()
{
    stop
    for i in {3..1}
    do
        echo -n "$i "
        sleep 1
    done
    echo 0
    start
}

function usage()
{
    echo "Usage: $0 {start|stop|restart|status|stop -f}"
    echo "Example: $0 start"
    exit 1
}

case $1 in
    start)
    start;;

    stop)
    stop;;

    restart)
    restart;;

    status)
    status;;

    *)
    usage;;
esac