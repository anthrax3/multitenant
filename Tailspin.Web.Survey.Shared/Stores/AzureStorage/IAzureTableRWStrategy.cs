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
    using System.Data.Services.Client;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table.DataServices;

    public interface IAzureTableRWStrategy
    {
        void ReadEntity(TableServiceContext context, ReadingWritingEntityEventArgs args);
        void WriteEntity(TableServiceContext context, ReadingWritingEntityEventArgs args);
    }
}
