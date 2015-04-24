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
    using Microsoft.WindowsAzure;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using System.IO;

    public class FilesBlobContainer : AzureBlobContainer<byte[]>
    {
        private readonly string contentType;

        public FilesBlobContainer(CloudStorageAccount account, string containerName, string contentType)
            : base(account, containerName)
        {
            this.contentType = contentType;
        }

        public override void EnsureExist()
        {
            this.StorageRetryPolicy.ExecuteAction(() =>
            {
                this.Container.CreateIfNotExists();//hieu
                this.Container.SetPermissions(new BlobContainerPermissions { PublicAccess = BlobContainerPublicAccessType.Blob });
            });
        }

        protected override byte[] ReadObject(CloudBlockBlob blob)
        {
            blob = Container.GetBlockBlobReference(blob.Name);
            //return blob.DownloadByteArray();//hieu
            using (var ms = new MemoryStream())
            {
                blob.DownloadToStream(ms);
                ms.Position = 0;
                return ms.ToArray();
            }
        }

        protected override void WriteOject(CloudBlockBlob blob, BlobRequestOptions options, AccessCondition condition, byte[] obj)
        {
            blob.Properties.ContentType = this.contentType;
            //blob.UploadByteArray(obj, options);//hieu
            blob.UploadFromByteArray(obj, 0, obj.Length, condition, options);
        }

        protected override byte[] BinarizeObjectForStreaming(BlobProperties properties, byte[] obj)
        {
            properties.ContentType = this.contentType;
            return obj;
        }
    }
}