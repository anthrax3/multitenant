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

    public class AzureTable<T> : AzureStorageWithRetryPolicy, IAzureTable<T> where T : TableEntity, new()
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
                return this.AzureCloudTable.CreateQuery<T>().AsQueryable<T>();
            }
        }

        public IAzureTableRWStrategy ReadWriteStrategy { get; set; }
                
        public void EnsureExist()
        {
            this.StorageRetryPolicy.ExecuteAction(() =>
            { 
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
        }
        /// <summary>
        /// No use
        /// </summary>
        /// <returns></returns>
        private CloudTable CreateContext()
        {
            CloudTableClient client = this.account.CreateCloudTableClient();
            var context = client.GetTableReference(tableName);
            return context;           
        }

        private class PartitionKeyComparer : IEqualityComparer<TableEntity>
        {
            public bool Equals(TableEntity x, TableEntity y)
            {
                return string.Compare(x.PartitionKey, y.PartitionKey, true, System.Globalization.CultureInfo.InvariantCulture) == 0;
            }           

            public int GetHashCode(TableEntity obj)
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