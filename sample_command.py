ffmpeg -hide_banner -loglevel debug -y \
-thread_queue_size 8192 -analyzeduration 20M -probesize 20M \
-ss 3539.77 -re -fflags +genpts+igndts+discardcorrupt+fastseek \
-threads 4 -avoid_negative_ts make_zero \
-progress /app/logs/ffmpeg/woody_progress.log \
-i /mnt/videos/streaming_awesomeness/woody/ready_to_stream/Allen__Woody_1977_Annie_Hall.0075686.mp4 \
-c:v copy -c:a copy -sn -dn \
-map 0:v:0 -map 0:a:0? -max_muxing_queue_size 4096 \
-f hls -hls_time 2 -hls_list_size 15 -hls_delete_threshold 5 \
-hls_flags delete_segments+append_list+program_date_time+independent_segments+split_by_time+round_durations \
-hls_allow_cache 1 -start_number 0 -hls_segment_type mpegts \
-max_delay 2000000 -hls_init_time 2 \
-hls_segment_filename /app/hls/woody/segment_%d.ts \
/app/hls/woody/playlist.m3u8