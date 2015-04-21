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
    using System.Linq;
    using Microsoft.WindowsAzure;
    using Microsoft.WindowsAzure.Storage;
    using Tailspin.Web.Survey.Shared.Stores.Azure;
    //using Microsoft.WindowsAzure.Storage.Table.DataServices;
    using Microsoft.WindowsAzure.Storage.Table;

    public interface IAzureTable<T> : IAzureObjectWithRetryPolicyFactory where T : TableEntity//TableServiceEntity hieu
    {
        CloudTable AzureCloudTable { get; }
        IQueryable<T> Query { get; }
        CloudStorageAccount Account { get; }

        void EnsureExist();
        void Add(T obj);
        void Add(IEnumerable<T> objs);
        void AddOrUpdate(T obj);
        void AddOrUpdate(IEnumerable<T> objs);
        void Delete(T obj);
        void Delete(IEnumerable<T> objs);
    }
}