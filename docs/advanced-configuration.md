# Advanced configuration and debugging for Cromwell on Azure
This article describes advanced features that allow customization and debugging of Cromwell on Azure.

## Expand data disk for MySQL database storage for Cromwell
To ensure that no data is corrupted for MySQL backed storage for Cromwell, Cromwell on Azure mounts MySQL files on to an Azure Managed Data Disk of size 32G. In case there is a need to increase the size of this data disk, follow instructions [here](https://docs.microsoft.com/en-us/azure/virtual-machines/linux/expand-disks#expand-an-azure-managed-disk).

## Connect to the host VM
To get logs from all the docker containers or to use the Cromwell REST API endpoints, you may want to connect to the Linux host VM. At installation, a user is created to allow managing the host VM with username "vmadmin". The password is randomly generated and shown during installation. If you need to reset your VM password, you can do this using the Azure Portal or by following these [instructions](https://docs.microsoft.com/en-us/azure/virtual-machines/troubleshooting/reset-password). 

![Reset password](/docs/screenshots/resetpassword.PNG)

To connect to your host VM, you can either
1. Construct your ssh connection string if you have the VM name `ssh vmadmin@<hostname>` OR
2. Navigate to the Connect button on the Overview blade of your Azure VM instance, then copy the ssh connection string. 

Paste the ssh connectiong string in a command line, PowerShell or terminal application to log in.

![Connect with SSH](/docs/screenshots/connectssh.PNG)

### How to get container logs to debug issues
The host VM is running multiple docker containers that enable Cromwell on Azure - mysql, broadinstitute/cromwell, cromwellonazure/tes, cromwellonazure/triggerservice. On rare occasions, you may want to debug and diagnose issues with the docker containers. After logging in to the VM, run: 
```
sudo docker ps
```

This command will list the names of all the docker containers currently running. To get logs for a particular container, run: 
```
sudo docker logs 'containerName'
```

### Access the Cromwell REST API directly from Linux host VM
Cromwell is run in server mode on the Linux host VM and can be accessed via curl as described below:

***Get all workflows***<br/>

`curl -X GET "http://localhost:8000/api/workflows/v1/query" -H  "accept: application/json"`<br/>

***Get specific workflow's status by id***<br/>
`curl -X GET "http://localhost:8000/api/workflows/v1/{id}/status" -H  "accept: application/json"`<br/>

***Get call-caching difference between two workflow calls***<br/>
`curl -X GET "http://localhost:8000/api/workflows/v1/callcaching/diff?workflowA={workflowId1}&callA={workflowName.callName1}&workflowB={workflowId2}&callB={workflowName.callName2}" -H  "accept: application/json"`<br/>

You can perform other Cromwell API calls following a similar pattern. To see all available API endpoints, see Cromwell's REST API [here](https://cromwell.readthedocs.io/en/stable/api/RESTAPI/)

## Connect to custom Azure resources
Cromwell on Azure uses [managed identities](https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/overview) to allow the host VM to connect to Azure resources in a simple and secure manner.  At the time of installation, a managed identity is created and associated with the host VM. You can find the identity via the Azure Portal by searching for the VM name in Azure Active Directory, under "All Applications". Or you may use Azure CLI `show` command as described [here](https://docs.microsoft.com/en-us/cli/azure/vm/identity?view=azure-cli-latest#az-vm-identity-show).

To allow the host VM to connect to **custom** Azure resources like Storage Account, Batch Account etc. you can use the [Azure Portal](https://docs.microsoft.com/en-us/azure/role-based-access-control/role-assignments-portal) or [Azure CLI](https://docs.microsoft.com/en-us/azure/role-based-access-control/role-assignments-cli) to find the managed identity of the host VM and add it as a Contributor to the required Azure resource.<br/>

For convenience, some configuration files are hosted on your Cromwell on Azure Storage account, in the "configuration" container - `containers-to-mount`, and `cromwell-application.conf`. You can modify and save these file using Azure Portal UI "Edit Blob" option or simply upload a new file to replace the existing one.

For these changes to take effect, be sure to restart your Cromwell on Azure VM through the Azure Portal UI or run `sudo reboot`.

![Restart VM](/docs/screenshots/restartVM.png)

### Use private docker containers 
Cromwell on Azure supports private docker images for your WDL tasks hosted on [Azure Container Registry or ACR](https://docs.microsoft.com/en-us/azure/container-registry/).
To allow the host VM to use an ACR, add the VM identity as a Contributor to the Container Registry via Azure Portal or Azure CLI.<br/>

### Mount another storage account
Navigate to the "configuration" container in the Cromwell on Azure Storage account. Replace YOURSTORAGEACCOUNTNAME with your storage account name and YOURCONTAINERNAME with your container name in the `containers-to-mount` file below:
```
/YOURSTORAGEACCOUNTNAME/YOURCONTAINERNAME/
```
Add this to the end of file and save your changes.<br/>

To allow the host VM to write to a Storage account, add the VM Identity as a Contributor to the Storage Account via Azure Portal or Azure CLI.<br/>

Alternatively, you can choose to add a [SAS url for your desired container](https://docs.microsoft.com/en-us/azure/storage/common/storage-sas-overview) to the end of the `containers-to-mount` file. This is also applicable if your VM cannot be granted Contributor access to the Storage account because the two resources are in different Azure tenants
```
https://<yourstorageaccountname>.blob.core.windows.net:443/<yourcontainername>?<sastoken>
```

When using the newly mounted storage account in your inputs JSON file, use the path `"/container-mountpath/filepath.extension"`, where `container-mountpath` is `/YOURSTORAGEACCOUNTNAME/YOURCONTAINERNAME/`.

For these changes to take effect, be sure to restart your Cromwell on Azure VM through the Azure Portal UI.

### Change batch account
Log on to the host VM using the ssh connection string as described above. Replace `BatchAccountName` environment variable for the "tes" service in the `docker-compose.yml` file with the name of the desired Batch account and save your changes.<br/>

```
cd /cromwellazure/
sudo nano docker-compose.yml
# Modify the BatchAccountName and save the file
```
To allow the host VM to use a batch account, add the VM identity as a Contributor to the Azure Batch account via Azure Portal or Azure CLI.<br/>
To allow the host VM to read prices and information about types of machines available for the batch account, add the VM identity as a Billing Reader to the subscription with the configured Batch Account.

For these changes to take effect, be sure to restart your Cromwell on Azure VM through the Azure Portal UI or run `sudo reboot`. or run `sudo reboot`.

### Use dedicated VMs for all your tasks
By default, we are using an environment variable `UsePreemptibleVmsOnly` set to true, to always use low priority Azure Batch nodes.<br/>

If you prefer to use dedicated Azure Batch nodes, log on to the host VM using the ssh connection string as described above. Replace `UsePreemptibleVmsOnly` environment variable for the "tes" service to "false" in the `docker-compose.yml` file and save your changes.<br/>

```
cd /cromwellazure/
sudo nano docker-compose.yml
# Modify UsePreemptibleVmsOnly to false and save the file
```

For these changes to take effect, be sure to restart your Cromwell on Azure VM through the Azure Portal UI or run `sudo reboot`.

### Connect Cromwell on Azure to a managed instance of Azure MySQL
**Create Azure MySQL server**<br/>
Create a managed instance for Azure MySQL following instructions [here](https://docs.microsoft.com/en-us/azure/mysql/quickstart-create-mysql-server-database-using-azure-portal). In the Connection security settings, add a rule to allow the host VM public IP address access to the MySQL server.<br/>

**Create a user for Cromwell**<br/>
Connect with admin username/password to the MySQL database. Replace "yourMySQLServerName" with your MySQL server name and run the following:

```
CREATE USER 'cromwell'@'localhost' IDENTIFIED BY 'cromwell'; 
GRANT ALL PRIVILEGES ON cromwell_db.* TO 'cromwell'@'localhost' WITH GRANT OPTION; 
CREATE USER 'cromwell'@'%' IDENTIFIED BY 'cromwell'; 
GRANT ALL PRIVILEGES ON cromwell_db.* TO 'cromwell'@'%' WITH GRANT OPTION;
CREATE USER 'cromwell'@'yourMySQLServerName.mysql.database.azure.com' IDENTIFIED BY 'cromwell'; 
GRANT ALL PRIVILEGES ON cromwell_db.* TO 'cromwell'@'yourMySQLServerName.mysql.database.azure.com' WITH GRANT OPTION;
CREATE USER 'cromwell'@'yourMySQLServerName.mysql.database.azure.com' IDENTIFIED BY 'cromwell'; 
GRANT ALL PRIVILEGES ON cromwell_db.* TO 'cromwell'@'yourMySQLServerName.mysql.database.azure.com' WITH GRANT OPTION;
create database cromwell_db
flush privileges
```

**Connect Cromwell to the database by modifying the Cromwell configuration file**<br/>
Navigate to the "configuration" container in the Cromwell on Azure Storage account. Replace "yourMySQLServerName" with your MySQL server name in the `cromwell-application.conf` file under the database connection settings. <br/>

```
cd /cromwell-app-config
sudo nano cromwell-application.conf
```

Find the database section and make the changes:
```
database {
  db.url = "jdbc:mysql://<yourMySQLServerName>.mysql.database.azure.com:3306/cromwell_db?useSSL=false&rewriteBatchedStatements=true&allowPublicKeyRetrieval=true&serverTimezone=UTC"
  db.user = "cromwell@yourMySQLServerName"
  db.password = "cromwell"
  db.driver = "com.mysql.cj.jdbc.Driver"
  profile = "slick.jdbc.MySQLProfile$"
  db.connectionTimeout = 15000
}

```
For these changes to take effect, be sure to restart your Cromwell on Azure VM through the Azure Portal UI or run `sudo reboot`.

Learn more about Cromwell configuration options [here](https://cromwell.readthedocs.io/en/stable/Configuring/)<br/>
