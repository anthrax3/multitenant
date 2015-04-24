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
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;

    internal class AzureBlob : IListBlobItemWithName //: CloudBlob, IListBlobItemWithName
    {
       //internal AzureBlob(CloudBlob source) : base(source) { }
        private CloudBlockBlob blockBlob;
        internal AzureBlob(CloudBlockBlob source)  {
            this.blockBlob = source;
        }

        public string Name
        {
            get { return blockBlob.Name; }
        }

        public CloudBlobContainer Container
        {
            get { throw new System.NotImplementedException(); }
        }

        public CloudBlobDirectory Parent
        {
            get { throw new System.NotImplementedException(); }
        }

        public StorageUri StorageUri
        {
            get { throw new System.NotImplementedException(); }
        }

        public System.Uri Uri
        {
            get { throw new System.NotImplementedException(); }
        }
    }
}
