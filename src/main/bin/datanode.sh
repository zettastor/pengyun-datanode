#!/bin/bash 
#
# Copyright (c) 2022. PengYunNetWork
#
# This program is free software: you can use, redistribute, and/or modify it
# under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
# as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
#  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
#  You should have received a copy of the GNU Affero General Public License along with
#  this program. If not, see <http://www.gnu.org/licenses/>.
#

# Absolute path this script is in, thus /home/user/bin

SCRIPTPATH=$(cd "$(dirname "$0")"; pwd)
ROOTPATH=$(dirname "$SCRIPTPATH")

for param in $@
do
    if [ "$param" = "test" ]
    then
        PERFORMANCE_TESTING=$param
    fi

    if [ "$param" = "yourkit_enabled" ]
    then
        YOURKIT_ENABLED=$param      
    fi

    if [ "${param:0:5}" = "group" ]
    then
        GROUP_ID=${param:5}
    fi

    if [ "$param" = "arbiter" ]
    then
        ARBITER=$param
    fi
done


if
[ "$ARBITER" = "arbiter" ]
then
   JVM_CONFIG_FILE=$ROOTPATH/config/arbiter_jvm.properties
   dn_info_placeholder="arbiter_info"
else
   JVM_CONFIG_FILE=$ROOTPATH/config/jvm.properties
   dn_info_placeholder="datanode_info"
fi
METRIC_CONFIG_FILE=$ROOTPATH/config/metric.properties
DATANODE_CONFIG_FILE=$ROOTPATH/config/datanode.properties

findStr()
{
    local target=$1
    local file=$2
    #echo target : $target
    #echo file : $file
    sed '/^\#/d' ${file} | grep ${target} | sed -e 's/ //g' |
        while read LINE
        do
            local KEY=`echo $LINE | cut -d "=" -f 1`
            local VALUE=`echo $LINE | cut -d "=" -f 2`
            [ ${KEY} = ${target} ] && {
                local UNKNOWN_NAME=`echo $VALUE | grep '\${.*}' -o | sed 's/\${//' | sed 's/}//'`
                if [ $UNKNOWN_NAME ];then
                    local UNKNOWN_VALUE=`findStr ${UNKNOWN_NAME} ${file}`
                    echo ${VALUE} | sed s/\$\{${UNKNOWN_NAME}\}/${UNKNOWN_VALUE}/
                else
                    echo $VALUE
                fi
                return 
            }
        done
    return
}

xms=$( findStr initial.mem.pool.size $JVM_CONFIG_FILE )
xmx=$( findStr max.mem.pool.size $JVM_CONFIG_FILE )

lc_all_bak="$LC_ALL"
export LC_ALL=C

max_direct_memory_size="$( free -g | grep Mem | awk '{print $2}' )G"
#max_direct_memory_size=$( findStr max.direct.memory.size $JVM_CONFIG_FILE )

version=`java11 -version 2>&1`
v8="version \"1.8"
v11="version \"11"
if [[ $version =~ "command not found" ]];
then
    version=`java -version 2>&1`
    java="java"
else
    java="java11"
fi

export LC_ALL="$lc_all_bak"

max_gc_pause_ms=$( findStr max.gc.pause.ms $JVM_CONFIG_FILE )
gc_pause_interval_ms=$( findStr gc.pause.internal.ms $JVM_CONFIG_FILE )
parallel_gc_threads=$( findStr parallel.gc.threads $JVM_CONFIG_FILE )
conc_gc_threads=$( findStr conc.gc.threads $JVM_CONFIG_FILE )
initiating_heap_occupancy_percent=$( findStr initiating.heap.occupancy.percent $JVM_CONFIG_FILE )
g1_rs_updating_pause_time_percent=$( findStr g1.rs.updating.pause.time.percent $JVM_CONFIG_FILE )
#g1_conc_refinement_threads=$( findStr g1.conc.refinement.threads $JVM_CONFIG_FILE )
g1_new_size_percent=$( findStr g1.new.size.percent $JVM_CONFIG_FILE )
g1_max_new_size_percent=$( findStr g1.max.new.size.percent $JVM_CONFIG_FILE )
#############################################################
#### only initial code cache size and metadata space is configured
#### their max values are still defaults
initial_codecache_size=$( findStr initial.codecache.size $JVM_CONFIG_FILE )
init_metaspace_size=$( findStr initial.metaspace.size $JVM_CONFIG_FILE )

#netty options
netty_leak_detection_level=$(findStr netty.leak.detection.level $JVM_CONFIG_FILE)
netty_leak_detection_target_records=$(findStr netty.leak.detection.target.records $JVM_CONFIG_FILE)

metric_enable=$( findStr metric.enable $METRIC_CONFIG_FILE )
metric_enable_profiles=$( findStr metric.enable.profiles $METRIC_CONFIG_FILE )
data_node_port=$( findStr app.main.endpoint $DATANODE_CONFIG_FILE )
debug_port=$(($data_node_port+2000))

#### trace jvm memory
jvm_memory_trace_level=$( findStr jvm.memory.trace.level $JVM_CONFIG_FILE )

#### new_ratio and survivor_ratio are not used any more ####
new_ratio=$( findStr new.generation.ratio $JVM_CONFIG_FILE )
survivor_ratio=$( findStr survivor.ratio $JVM_CONFIG_FILE )
###########################################################

enableMetrics="-Dmetric.enable=$metric_enable -Dmetric.enable.profiles=$metric_enable_profiles"
dnPlaceHolder="-Ddatanode.info.placeholder=$dn_info_placeholder"

initializeSpaceOptions="-XX:+AlwaysPreTouch -XX:MetaspaceSize=$init_metaspace_size -XX:InitialCodeCacheSize=$initial_codecache_size -XX:ReservedCodeCacheSize=240m -XX:MaxDirectMemorySize=$max_direct_memory_size"

performanceOptions="-XX:G1RSetUpdatingPauseTimePercent=$g1_rs_updating_pause_time_percent -XX:ConcGCThreads=$conc_gc_threads -XX:InitiatingHeapOccupancyPercent=$initiating_heap_occupancy_percent"

sharedOptions="-server -Xms$xms -Xmx$xmx -XX:-OmitStackTraceInFastThrow -XX:+UseG1GC -XX:MaxGCPauseMillis=$max_gc_pause_ms -XX:GCPauseIntervalMillis=$gc_pause_interval_ms -XX:ParallelGCThreads=$parallel_gc_threads -XX:+ParallelRefProcEnabled -XX:+HeapDumpOnOutOfMemoryError $enableMetrics $dnPlaceHolder"

nettyOptions="-Dio.netty.leakDetectionLevel=$netty_leak_detection_level -Dio.netty.leakDetection.targetRecords=$netty_leak_detection_target_records"

if [[ $version =~ $v8 ]];
then
    printGCOptions="-Xloggc:logs/gc.log -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+UnlockDiagnosticVMOptions -XX:+PrintSafepointStatistics -XX:PrintSafepointStatisticsCount=1 -XX:+LogVMOutput -XX:LogFile=logs/vm.log -XX:+G1SummarizeConcMark -XX:+G1SummarizeRSetStats -XX:G1SummarizeRSetStatsPeriod=1 -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=20 -XX:GCLogFileSize=100M"
elif [[ $version =~ $v11 ]];
then
    #printGCOptions="-Xlog:gc*=trace,gc+tlab=info,gc+heap+region=info,safepoint*:file=logs/gc.log:uptime,time,level,tags,tid -XX:+UnlockDiagnosticVMOptions -XX:+PrintSafepointStatistics -XX:PrintSafepointStatisticsCount=1 -XX:+LogVMOutput -XX:LogFile=logs/vm.log"
    printGCOptions="-Xlog:gc*,gc=trace,gc+marking=trace,gc+remset*=trace,gc+task*=trace,gc+jni=trace,gc+phases*=trace,gc+ref*=trace,gc+age=trace,gc+humongous=trace,gc+stringtable=trace,safepoint*:file=logs/gc.log:uptime,time,level,tags,tid:filecount=20,filesize=100M -XX:+UnlockDiagnosticVMOptions -XX:+PrintSafepointStatistics -XX:PrintSafepointStatisticsCount=1 -XX:+UnlockExperimentalVMOptions -XX:G1NewSizePercent=$g1_new_size_percent -XX:G1MaxNewSizePercent=$g1_max_new_size_percent"
fi


if [ "$YOURKIT_ENABLED" = "yourkit_enabled" -o -f "yourkitFlag" ]
then
	touch "yourkitFlag"
	enableYKAgent="-agentpath:/opt/yourkit/libyjpagent.so"
else
	enableYKAgent=""
fi

if [ "$ARBITER" = "arbiter" ]
then
	nettyOptions=""
	launcher="py.datanode.ArbiterLauncher"
else
	launcher="py.datanode.Launcher"
fi

$java -noverify $sharedOptions $nettyOptions -XX:OnOutOfMemoryError="kill -9 %p" -XX:NativeMemoryTracking=$jvm_memory_trace_level $enableYKAgent $printGCOptions $performanceOptions $initializeSpaceOptions -Xdebug -cp "$ROOTPATH:$ROOTPATH/lib/*:$ROOTPATH/config" $launcher $ROOTPATH $GROUP_ID 2>&1 >> /dev/null 
