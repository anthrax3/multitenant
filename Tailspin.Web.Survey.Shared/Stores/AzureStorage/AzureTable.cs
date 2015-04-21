//===============================================================================
// Microsoft patterns & practices
// Windows Azure Architecture Guide
//===============================================================================
// Copyright © Microsoft Corporation.  All rights reserved.
// This code released under the terms of the 
// Microsoft patterns & practices license (http://wag.codeplex.com/license)
//===============================================================================


namespace Tailspin.Web.Survey.Shared.Stores.AzureStorage
{
    using System.Collections.Generic;
    using System.Data.Services.Client;
    using System.Linq;
    using Microsoft.WindowsAzure;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table.Queryable;
    using Tailspin.Web.Survey.Shared.Helpers;
    using Microsoft.WindowsAzure.Storage.Table;
    //using Microsoft.WindowsAzure.Storage.Table.DataServices;

    public class AzureTable<T> : AzureStorageWithRetryPolicy, IAzureTable<T> where T : TableEntity, new()// TableServiceEntity
    {
        private readonly string tableName;
        private readonly CloudStorageAccount account;

        public AzureTable(CloudStorageAccount account)
            : this(account, typeof(T).Name)
        {
        }

        public AzureTable(CloudStorageAccount account, string tableName)
        {
            this.tableName = tableName;
            this.account = account;
        }

        public CloudStorageAccount Account
        {
            get
            {
                return this.account;
            }
        }
        
        public IQueryable<T> Query
        {
            get
            {
                //TableQuery<T> query = new TableQuery<T>();
                return this.AzureCloudTable.CreateQuery<T>().AsQueryable<T>();
                //(from book in this.AzureCloudTable.CreateQuery<T>()
  // where book.PartitionKey == "hardback"
  // select book).AsTableQuery<T>();
                //var query = new TableQuery<T>();
                //return this.AzureCloudTable.CreateQuery<T>();
                //var fluentQuery = new TableQuery<T>();
                //var fluentResult = this.AzureCloudTable.ExecuteQuery(fluentQuery);

                //var iQueryableQuery = from ent in this.AzureCloudTable.CreateQuery<T>()
                //                      where ent.PartitionKey == "some value"
                //                      select ent;
                //var iQueryableResult = fluentResult.AsQueryable<T>();
                //return new TableQuery<T>();//.AsQueryable<T>(this.tableName); //hieu
            }
            
            //    TableQuery query = new TableQuery<T>();
            //    query.
            //    TableServiceContext context = this.CreateContext();
            //    return context.CreateQuery<T>(this.tableName).AsTableServiceQuery();
            //}
        }

        public IAzureTableRWStrategy ReadWriteStrategy { get; set; }
                
        public void EnsureExist()
        {
            this.StorageRetryPolicy.ExecuteAction(() =>
            {
                //hieu
                //var cloudTableClient = new CloudTableClient(this.account.TableEndpoint.ToString(), this.account.Credentials);
                CloudTableClient cloudTableClient = this.account.CreateCloudTableClient();
                var cloudTable = cloudTableClient.GetTableReference(this.tableName);
                cloudTable.CreateIfNotExists();
            });
        }

        public void Add(T obj)
        {
          this.Add(new[] { obj });
        }

        public void Add(IEnumerable<T> objs)
        {
            var tableClient = this.Account.CreateCloudTableClient();
            foreach (var obj in objs)
            {
                CloudTable table = tableClient.GetTableReference(this.tableName);
                table.Execute(TableOperation.Insert(obj));
            }   
            
            //TableServiceContext context = this.CreateContext();

            //foreach (var obj in objs)
            //{
            //    context.AddObject(this.tableName, obj);
            //}

            //var saveChangesOptions = SaveChangesOptions.None;
            //if (objs.Distinct(new PartitionKeyComparer()).Count() == 1)
            //{
            //    saveChangesOptions = SaveChangesOptions.Batch;
            //}

            //this.StorageRetryPolicy.ExecuteAction(() => context.SaveChanges(saveChangesOptions));
        }

        public void AddOrUpdate(T obj)
        {
            this.AddOrUpdate(new[] { obj });
        }

        public void AddOrUpdate(IEnumerable<T> objs)
        {
            foreach (var obj in objs)
            {
                TableOperation insertOrMergeOperation = TableOperation.InsertOrMerge(obj);
                this.AzureCloudTable.Execute(insertOrMergeOperation);
                /*
                var existingObj = default(T);
                var added = this.StorageRetryPolicy.ExecuteAction<bool>(() =>
                    {
                        var result = false;

                        try
                        {
                            existingObj = (from o in this.Query
                                           where o.PartitionKey == obj.PartitionKey
                                           && o.RowKey == obj.RowKey
                                           select o).SingleOrDefault();
                        }
                        catch (DataServiceQueryException ex)
                        {
                            var dataServiceClientException = ex.InnerException as DataServiceClientException;
                            if (dataServiceClientException != null)
                            {
                                if (dataServiceClientException.StatusCode == 404)
                                {
                                    TraceHelper.TraceWarning(ex.TraceInformation());

                                    this.Add(obj);
                                    result = true;
                                }
                                else
                                {
                                    TraceHelper.TraceError(ex.TraceInformation());
                                    throw;
                                }
                            }
                            else
                            {
                                TraceHelper.TraceError(ex.TraceInformation());
                                throw;
                            }
                        }

                        return result;
                    });

                if (added)
                {
                    return;
                }

                if (existingObj == null)
                {
                    this.Add(obj);
                }
                else
                {
                    //hieu
                    //TableServiceContext context = this.CreateContext();
                    //context.AttachTo(this.tableName, obj, "*");
                    //context.UpdateObject(obj);
                    //this.StorageRetryPolicy.ExecuteAction(() => context.SaveChanges(SaveChangesOptions.ReplaceOnUpdate));
                    //hieu: this is update, will rewrite by merge
                    TableOperation mergeOperation = TableOperation.Merge(obj);
                    this.AzureCloudTable.Execute(mergeOperation);
                }
                  */
            }
               
        }

        public void Delete(T obj)
        {
            this.Delete(new[] { obj });
        }

        public void Delete(IEnumerable<T> objs)
        {
            var tableClient = this.Account.CreateCloudTableClient();
            foreach (var obj in objs)
            {
                CloudTable table = tableClient.GetTableReference(this.tableName);
                TableOperation retrieveOperation = TableOperation.Retrieve<T>(obj.PartitionKey, obj.RowKey);
                // Execute the operation.
                TableResult retrievedResult = table.Execute(retrieveOperation);
                // Assign the result to a CustomerEntity.
                T deleteEntity = (T)retrievedResult.Result;
                // Create the Delete TableOperation.
                if (deleteEntity != null)
                {
                    TableOperation deleteOperation = TableOperation.Delete(deleteEntity);
                    // Execute the operation.
                    table.Execute(deleteOperation);                    
                }               
            }   
            //TableServiceContext context = this.CreateContext();//hieu
            //foreach (var obj in objs)
            //{
            //    context.AttachTo(this.tableName, obj, "*");
            //    context.DeleteObject(obj);
            //}

            //this.StorageRetryPolicy.ExecuteAction(() =>
            //    {
            //        try
            //        {
            //            context.SaveChanges();
            //        }
            //        catch (DataServiceRequestException ex)
            //        {
            //            var dataServiceClientException = ex.InnerException as DataServiceClientException;
            //            if (dataServiceClientException != null)
            //            {
            //                if (dataServiceClientException.StatusCode == 404)
            //                {
            //                    TraceHelper.TraceWarning(ex.TraceInformation());
            //                    return;
            //                }
            //            }

            //            TraceHelper.TraceError(ex.TraceInformation());

            //            throw;
            //        }
            //    });
        }

        private CloudTable CreateCloudTable()// CreateContext()
        {
            CloudTableClient client = this.account.CreateCloudTableClient();
            var context = client.GetTableReference(tableName);

            //var context = new TableServiceContext(this.account.TableEndpoint.ToString(), this.account.Credentials)
            //{
            //    // retry policy is handled by TFHAB
            //    RetryPolicy = RetryPolicies.NoRetry()
            //};

            //if (this.ReadWriteStrategy != null)
            //{
            //    context.ReadingEntity += (sender, args) => this.ReadWriteStrategy.ReadEntity(context, args);
            //    context.WritingEntity += (sender, args) => this.ReadWriteStrategy.WriteEntity(context, args);
            //}

            return context;
            //var context = new TableServiceContext(this.account.TableEndpoint.ToString(), this.account.Credentials)
            //{
            //    // retry policy is handled by TFHAB
            //    RetryPolicy = RetryPolicies.NoRetry()
            //};

            //if (this.ReadWriteStrategy != null)
            //{
            //    context.ReadingEntity += (sender, args) => this.ReadWriteStrategy.ReadEntity(context, args);
            //    context.WritingEntity += (sender, args) => this.ReadWriteStrategy.WriteEntity(context, args);
            //}

            //return context;
        }

        private class PartitionKeyComparer : IEqualityComparer<TableEntity>// IEqualityComparer<TableServiceEntity> hieu
        {
            public bool Equals(TableEntity x, TableEntity y)
            {
                return string.Compare(x.PartitionKey, y.PartitionKey, true, System.Globalization.CultureInfo.InvariantCulture) == 0;
            }
            //public bool Equals(TableServiceEntity x, TableServiceEntity y) hieu
            //{
            //    return string.Compare(x.PartitionKey, y.PartitionKey, true, System.Globalization.CultureInfo.InvariantCulture) == 0;
            //}

            public int GetHashCode(TableEntity obj)//GetHashCode(TableServiceEntity obj) hieu
            {
                return obj.PartitionKey.GetHashCode();
            }
        }      

        public CloudTable AzureCloudTable
        {
            get {
                CloudTableClient client = this.account.CreateCloudTableClient();
                return client.GetTableReference(this.tableName);
            }
        }
    }
}