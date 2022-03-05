BASEDIR=$(dirname "$0")
cd ${BASEDIR}/..
nohup java -Dapp=dbsync -cp .:lib/* com.louyj.dbsync.DbSyncLauncher config/app.yaml > logs/dbsync.out 2>&1