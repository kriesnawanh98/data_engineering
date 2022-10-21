# Uploading Files from Google Compute Engine (GCE) VMs to Google Cloud Storage (GCS)
I had a bit of trouble trying to configure permissions to upload files from my Google Compute Engine instance to my Google Cloud Storage bucket.
The process isn't as intuitive as you think. There are a few permissions issues that need to be configured before this can happen. Here are the steps I took to get things working.


Let's say you want to upload `yourfile.txt` to a GCS bucket from your virtual machine.
You can use the `gsutil` command line tool that comes installed on all GCE instances.

If you've never used the `gcloud` or `gsutil` command line tools on this machine before, you will need to initialize them with a service account.


## Set Up Your Service Account
On the GCE instance run the following to set up:

```bash
gcloud init
```

The setup will ask you to choose the account you would like to use to perform operations for this configuration, and give you two options:

1. 1234567890-compute@developer.gserviceaccount.com
2. Log in with a new account

Choose number 1 to use a service account. If this is a shared machine and you log in with your personal account, your credentials could be used by anyone else on the machine.

Once logged in, try uploading your file with the `gsutil` command, which might look like this:

```bash
gsutil cp /home/you/yourfile.txt gs://your-bucket
```

You will notice you're faced with a `AccessDeniedException: 403 Insufficient Permission` message, and despite your best efforts it's difficult to debug.


## Enabling API Access for the GCE Instance
Next, you will need to enable API access for the virtual machine. By default your machine should have read access to the buckets in the same project, but configuration is required before you can write to them.

Navigate to [console.cloud.google.com](https://console.cloud.google.com/compute/instances?) and select your project from the drop down menu. Next, select your virtual machine and click `STOP` in the top menu bar.

Once your virtual machine has been stopped, click on it's name and then `EDIT` in the top menu bar.

Scroll down until you see a header called `Access Scopes`, which will likely be on the `Allow default access` selection. Select `Set access for each API` as your option, then scroll down until you see `Storage`, which is likely set on `READ`: change it to `READ WRITE`, or whatever you feel is necessary for your use case.

Save your changes and restart your virtual machine.

## Removing gsutil Cache
Once you've restarted your virtual machine, try to upload the file again:

```bash
gsutil cp /home/you/yourfile.txt gs://your-bucket
```

If you encounter the `AccessDeniedException: 403 Insufficient Permission` message again, navigate to your current home directory, and remove the .gsutil cache folder.

```bash
rm -r ~/.gsutil
```

You should now be able to upload successfully to your storage bucket.

```bash
gsutil cp /home/you/yourfile.txt gs://your-bucket
...
Operation completed over 1 objects.
```

## Enabling IAM Permissions
_(This section shouldn't be necessary - I am able to upload without explicit permissions set on the account in IAM)_

If the above doesn't work, you may need to enable additional IAM permissions for the service account.

First, find your service account with the following command
```bash
gcloud config list account
```
```bash
...
1234567890-compute@developer.gserviceaccount.com
```

Navigate to [console.cloud.google.com](https://console.cloud.google.com/iam-admin/iam?) and select your project from the drop down menu.

Find your service account in the members list, and click the edit pencil on the right hand side. Then add any permissions as needed.