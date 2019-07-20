# sgb - simple google backup
Like rsync, it recursively syncs the backup directory from config to your cloud storage bucket.

It has a -dry-run option so you can see what it will do before it does it.

It has a -force option that overcomes the hard coded check to not delete more than 100 files.

Put a config like this in $HOME/.config/sgb_config.yml
```
google_auth: $HOME/my-auth.json
project_id: my-project-id
bucket_name: my-storage-bucket
backup_root_dir: $HOME/important-files
upload_workers: 10
delete_workers: 1
crc_change_check: true
```

The auth file comes from google, see: https://cloud.google.com/iam/docs/creating-managing-service-account-keys#iam-service-account-keys-create-console

The project_id is your Google Cloud Project ID.

The bucket_name is the storage bucket you want to sync to.

The backup_root_dir is the local directory that will be synced.

It has go routines for upload and delete, number of each controlled in the config. You likely won't need more than 1 deleter.

It will notice if files have changed by doing a CRC check, which uses some CPU, if your files don't change without chaning size you may want to set this to false, in which case it will just check if the size has changed.

