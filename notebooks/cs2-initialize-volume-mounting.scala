// Databricks notebook source
import scala.util.control._

val configs = Map(
"fs.azure.account.auth.type" -> "OAuth",
"fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id" -> "1ccb9548-c831-4814-87df-15304b9dc902",
"fs.azure.account.oauth2.client.secret" -> dbutils.secrets.get(scope = "training-scope", key = "appsecret"),
"fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/c7b9726a-9a23-4998-a94a-d964382c2db3/oauth2/token")


val mounts = dbutils.fs.mounts()
val mountPath = "/mnt/data"
var isExist: Boolean = false
val outer = new Breaks;

outer.breakable {
  for(mount <- mounts) {
    if(mount.mountPoint == mountPath) {
      isExist = true;
      outer.break;
    }
  }
}

if(isExist) {
  println("Volume Mounting for Case Study Data Already Exist!")
}
else {
  dbutils.fs.mount(
    source = "abfss://casestudydata@dataanalyticsgenv2.dfs.core.windows.net/",
    mountPoint = "/mnt/data",
    extraConfigs = configs)
}