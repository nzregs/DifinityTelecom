// Databricks notebook source
// MAGIC %md
// MAGIC # Difinity Telecom - Secrets Management
// MAGIC 
// MAGIC ## Securely storing credentials for Azure Event Hubs, Blob Storage, Data Lake, Cosmos DB
// MAGIC 
// MAGIC We don't want to use secrets or passwords directly in the code of our notebooks, so instead we can use Databricks Secret Scopes, or Azure Key Vault backed Secret Scopes.  In this case we will use the Azure Key Vault backed scope.
// MAGIC 
// MAGIC [https://docs.azuredatabricks.net/user-guide/secrets/secret-scopes.html#create-an-azure-key-vault-backed-secret-scope](https://docs.azuredatabricks.net/user-guide/secrets/secret-scopes.html#create-an-azure-key-vault-backed-secret-scope)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC 1. Create a Key Vault in Azure: https://docs.microsoft.com/en-us/azure/key-vault/quick-create-portal
// MAGIC 
// MAGIC 2. Create a scope using the portal: https://westus.azuredatabricks.net/?o=8302412809171625#secrets/createScope
// MAGIC 
// MAGIC   * Use the URL for the key vault. E.g. [https://difinitytelecomkv.vault.azure.net/](https://difinitytelecomkv.vault.azure.net/)
// MAGIC   * Use the resource id for the __key vault__, NOT to a secret within the key vault. E.g. __/subscriptions/8d2e1127-5555-5555-5555-4dd399c8341b/resourceGroups/DifinityTelecom/providers/Microsoft.KeyVault/vaults/DifinityTelecomKV__
// MAGIC 
// MAGIC You might find [Azure Resource Explorer](https://resources.azure.com/) useful when looking up the Azure Resource ID for your secrets stored in Key Vault.  

// COMMAND ----------

// MAGIC %md
// MAGIC ### Use the Azure CLI to create secrets
// MAGIC 
// MAGIC NOTE: you __cannot__ use the databricks cli to create secrets in an Azure Key Vault backed scope.  You must use the Azure CLI
// MAGIC 
// MAGIC ```sh
// MAGIC az keyvault secret set --vault-name "DifinityTelecomKV" --name "MyServiceKKey" --value "hVFkk965BuUv"
// MAGIC ```
// MAGIC 
// MAGIC You can list the secrets using either the Azure CLI or the Databricks CLI, but you will not be able to retrieve the keys using the Databricks CLI

// COMMAND ----------

// MAGIC %md
// MAGIC ### Use the Databricks CLI to list secrets
// MAGIC 
// MAGIC You can access the Databricks CLI from the Azure Cloud Shell
// MAGIC 
// MAGIC 1) create a personal access token for the azure-cli to use. Make sure you copy the key when you create it, because you cannnot retrieve it again later. https://docs.azuredatabricks.net/api/latest/authentication.html#token-management
// MAGIC 2) Start the Azure cloud shell and configure a virtual environment for Databricks https://docs.microsoft.com/en-us/azure/azure-databricks/databricks-cli-from-azure-cloud-shell
// MAGIC 3) Run the following commands to configure databricks access:
// MAGIC 
// MAGIC ```sh
// MAGIC 
// MAGIC virtualenv -p /usr/bin/python2.7 databrickscli
// MAGIC source databrickscli/bin/activate
// MAGIC pip install databricks-cli
// MAGIC databricks configure --token
// MAGIC ```
// MAGIC 
// MAGIC 4) use the following commmand to list the secrets in your key vault:
// MAGIC 
// MAGIC ```sh
// MAGIC databricks secrets list --scope DifinityTelecomKV
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ### Consume the secrets in a Databricks Notebook

// COMMAND ----------

val Username = "difinitytel"
val Password = dbutils.secrets.get(scope = "DifinityTelecomKV", key = "DifinityTel-EventHub-TowerData")

print(Username)
print(Password)