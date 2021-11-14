# systemd integration

Create file `runkins.service` based on `runkins-sample.service` and
move/link it to `/etc/systemd/system/` folder. Then start the service:
```sh
systemctl daemon-reload
systemctl start runkins
```
