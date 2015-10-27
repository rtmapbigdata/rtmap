# If this file is placed at FLUME_CONF_DIR/flume-env.sh, it will be sourced
# during Flume startup.

# 
# Give Flume more memory and pre-allocate
# 
echo ----- adjust JVM heap size -----
export JAVA_OPTS="-Xms100m -Xmx4096m"

# for debug
echo JAVA_OPTS = $JAVA_OPTS
