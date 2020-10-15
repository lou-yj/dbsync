BASEDIR=$(dirname "$0")
cd ${BASEDIR}/..
nohup java -cp lib/* com.louyj.dbsync.DbSyncLanucher config/app.yaml &