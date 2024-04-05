# Conversion of the MapR-DB Sample C Application demo example in RUST

see https://support.hpe.com/hpesc/public/docDisplay?docId=a00edf60hen_us&docLocale=en_US&page=MapR-DB/Sample-C-app-DBtables.html

## Build

Requires libMapRClient for linking.

Linux
```
TO_EXTRACT=com/mapr/fs/native/Linux/x86_64/libMapRClient.so
```

MacOs
```
TO_EXTRACT=com/mapr/fs/native/Mac/x86_64/libMapRClient.dylib
```

Perform extraction
```
[[ -v TO_EXTRACT ]] \
&& cd /tmp/ \
&& curl https://repository.mapr.com/nexus/content/groups/mapr-public/com/mapr/hadoop/maprfs/7.5.0.0-mapr/maprfs-7.5.0.0-mapr.jar -o maprfs-7.5.0.0-mapr.jar \
&& unzip maprfs-7.5.0.0-mapr.jar ${TO_EXTRACT} \
&& cd - \
&& cp /tmp/${TO_EXTRACT} .
```

then build ...

```
cargo build
```

## Run

```
export MAPR_HOME=...
export MAPR_TICKET_LOCATION=...
target/debug/sample
```