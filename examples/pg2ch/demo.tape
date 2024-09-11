Output demo.gif

Require echo
Require trcli

Set Shell "bash"
Set FontSize 15
Set Width 1200
Set Height 800

Hide
    Type "tput rmam && bat transfer.yaml" Enter
Show

Sleep 5s

Type "trcli validate --transfer transfer.yaml --log-config=minimal" Sleep 500ms  Enter

Sleep 2s

Type "trcli check --transfer transfer.yaml --log-config=minimal" Sleep 500ms  Enter

Sleep 5s

Type "trcli activate --transfer transfer.yaml --log-config=minimal" Sleep 500ms  Enter

Sleep 5s

Type "clickhouse-client --host localhost --port 9000 --user default --password 'ch_password'" Enter

Sleep 1s

Type "select * from personas;" Sleep 500ms Enter

Sleep 10s

Type "echo 'all my favorite dudes in clickhouse now! <3'"

Sleep 10s
