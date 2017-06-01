# TODO: automate log/dir checking to confirm:
# 1) files sent in order
# 2) no file left behind
# 3) no duplicates

echo "-------------------------------------------------------------------------"
echo "ERRORS:"
grep "ERROR" $STS_HOME/data/log/messages/*/*
ototal=`cut -d ":" -f 1 $STS_HOME/data/log/outgoing_to/*/*/* | sort > /tmp/foo1`
 ouniq=`cut -d ":" -f 1 $STS_HOME/data/log/outgoing_to/*/*/* | sort | uniq > /tmp/foo2`
itotal=`cut -d ":" -f 1 $STS_HOME/data/log/incoming_from/*/*/* | sort > /tmp/foo3`
 iuniq=`cut -d ":" -f 1 $STS_HOME/data/log/incoming_from/*/*/* | sort | uniq > /tmp/foo4`
echo "-------------------------------------------------------------------------"
echo "OUT DUPLICATES:"
diff /tmp/foo1 /tmp/foo2
echo "-------------------------------------------------------------------------"
echo "IN DUPLICATES:"
diff /tmp/foo3 /tmp/foo4
echo "-------------------------------------------------------------------------"
echo "IN vs OUT:"
diff /tmp/foo2 /tmp/foo4
rm /tmp/foo*
echo "-------------------------------------------------------------------------"
echo "OUT DIR:"
find $STS_HOME/data/out -type f
echo "-------------------------------------------------------------------------"
echo "STAGE DIR:"
find $STS_HOME/data/stage -type f
echo "-------------------------------------------------------------------------"
echo "IN DIR:"
find $STS_HOME/data/in -type f | sort
