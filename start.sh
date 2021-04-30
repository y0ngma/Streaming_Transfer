#!usr/bin/sh
sleep 30
cd /home/pi/workspace/stream/
 /usr/local/bin/python3 /home/pi/workspace/stream/video_recoding.py &
 /usr/local/bin/python3 /home/pi/workspace/stream/send_to_s3.py &
