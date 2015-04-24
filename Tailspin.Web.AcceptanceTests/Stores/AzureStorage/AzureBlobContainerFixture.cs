//===============================================================================
// Microsoft patterns & practices
// Windows Azure Architecture Guide
//===============================================================================
// Copyright © Microsoft Corporation.  All rights reserved.
// This code released under the terms of the 
// Microsoft patterns & practices license (http://wag.codeplex.com/license)
//===============================================================================


namespace Tailspin.Web.AcceptanceTests.Stores.AzureStorage
{
    using System;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Web.Script.Serialization;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.WindowsAzure;
    using Microsoft.WindowsAzure.Storage;
    using Tailspin.Web.Survey.Shared.Helpers;
    using Tailspin.Web.Survey.Shared.Stores.AzureStorage;
    using Microsoft.WindowsAzure.Storage.Blob;

    [TestClass]
    public class AzureBlobContainerFixture
    {
        private const string AzureBlobTestContainer = "azureblobcontainerfortest";

        [ClassInitialize]
        public static void Initialize(TestContext context)
        {
            var azureBlobContainer = new TestAzureBlobContainer(
                CloudConfiguration.GetStorageAccount("DataConnectionString"),
                AzureBlobTestContainer);
            azureBlobContainer.EnsureExist();
        }

        [ClassCleanup]
        public static void Cleanup()
        {
            var azureBlobContainer = new TestAzureBlobContainer(
                 CloudConfiguration.GetStorageAccount("DataConnectionString"),
                 AzureBlobTestContainer);
            azureBlobContainer.DeleteContainer();
        }

        [TestMethod]
        public void DeleteShouldRemoveTheBlob()
        {
            var objId = Guid.NewGuid().ToString();

            var azureBlobContainer = new TestAzureBlobContainer(
                CloudConfiguration.GetStorageAccount("DataConnectionString"),
                AzureBlobTestContainer);
            azureBlobContainer.Save(objId, "testText");

            Assert.IsNotNull(azureBlobContainer.Get(objId));

            azureBlobContainer.Delete(objId);

            Assert.IsNull(azureBlobContainer.Get(objId));
        }

        [TestMethod]
        public void GetShouldRetrieveTheBlob()
        {
            var objId = Guid.NewGuid().ToString();

            var azureBlobContainer = new TestAzureBlobContainer(
                CloudConfiguration.GetStorageAccount("DataConnectionString"),
                AzureBlobTestContainer);
            azureBlobContainer.Save(objId, "testText");

            Assert.IsNotNull(azureBlobContainer.Get(objId));
        }

        [TestMethod]
        public void SaveShouldStoreTheBlob()
        {
            var objId = Guid.NewGuid().ToString();

            var azureBlobContainer = new TestAzureBlobContainer(
                CloudConfiguration.GetStorageAccount("DataConnectionString"),
                AzureBlobTestContainer);
            azureBlobContainer.Save(objId, "testText");

            Assert.IsNotNull(azureBlobContainer.Get(objId));
        }

        [TestMethod]
        public void GetBlobListReturnsAllBlobsInContainer()
        {
            var objId1 = Guid.NewGuid().ToString();
            var objId2 = Guid.NewGuid().ToString();

            var azureBlobContainer = new TestAzureBlobContainer(
                CloudConfiguration.GetStorageAccount("DataConnectionString"),
                AzureBlobTestContainer);
            azureBlobContainer.Save(objId1, "testText");
            azureBlobContainer.Save(objId2, "testText");

            var blobList = azureBlobContainer.GetBlobList().Select(b => b.Name).ToList();

            CollectionAssert.Contains(blobList, objId1);
            CollectionAssert.Contains(blobList, objId2);
        }

        [TestMethod]
        public void GetUriReturnsContainerUrl()
        {
            var objId = Guid.NewGuid().ToString();

            var azureBlobContainer = new TestAzureBlobContainer(
                CloudConfiguration.GetStorageAccount("DataConnectionString"),
                AzureBlobTestContainer);
            Assert.AreEqual(
                string.Format("http://127.0.0.1:10000/devstoreaccount1/{0}/{1}", AzureBlobTestContainer, objId),
                azureBlobContainer.GetUri(objId).ToString());
        }

        [TestMethod]
        public void LockFreeResourceReturnsTrueWithLockId()
        {
            var azureBlobContainer = new TestAzureBlobContainer(
                CloudConfiguration.GetStorageAccount("DataConnectionString"),
                AzureBlobTestContainer);

            var objId = Guid.NewGuid().ToString();
            azureBlobContainer.Save(objId, "testText");

            var lockContext = new PessimisticConcurrencyContext()
            {
                ObjectId = objId
            };

            Assert.IsTrue(azureBlobContainer.AcquireLock(lockContext));
            Assert.IsNotNull(lockContext.LockId);
        }

        [TestMethod]
        public void LockBusyResourceReturnsFalseWithNoLockId()
        {
            var azureBlobContainer = new TestAzureBlobContainer(
                CloudConfiguration.GetStorageAccount("DataConnectionString"),
                AzureBlobTestContainer);

            var objId = Guid.NewGuid().ToString();
            azureBlobContainer.Save(objId, "testText");

            var lockContext = new PessimisticConcurrencyContext()
            {
                ObjectId = objId
            };

            Assert.IsTrue(azureBlobContainer.AcquireLock(lockContext));
            Assert.IsNotNull(lockContext.LockId);

            Assert.IsFalse(azureBlobContainer.AcquireLock(lockContext));
            Assert.IsNull(lockContext.LockId);
        }

        [TestMethod]
        public void LockUnexistentResourceReturnsTrueWithNoLockId()
        {
            var azureBlobContainer = new TestAzureBlobContainer(
                CloudConfiguration.GetStorageAccount("DataConnectionString"),
                AzureBlobTestContainer);

            var lockContext = new PessimisticConcurrencyContext()
            {
                ObjectId = Guid.NewGuid().ToString()
            };

            Assert.IsTrue(azureBlobContainer.AcquireLock(lockContext));
            Assert.IsNull(lockContext.LockId);
        }

        [TestMethod]
        public void LockAndWriteResource()
        {
            var azureBlobContainer = new TestAzureBlobContainer(
                CloudConfiguration.GetStorageAccount("DataConnectionString"),
                AzureBlobTestContainer);

            var objId = Guid.NewGuid().ToString();
            azureBlobContainer.Save(objId, "testText");

            var context = new PessimisticConcurrencyContext() { ObjectId = objId };
            try
            {
                azureBlobContainer.AcquireLock(context);
                azureBlobContainer.Save(context, "testTextUpdated");
            }
            finally
            {
                azureBlobContainer.ReleaseLock(context);
            }

            Assert.AreEqual("testTextUpdated", azureBlobContainer.Get(objId));
        }

        [TestMethod]
        public void ReleaseResourceAllowsGettingNewLock()
        {
            var azureBlobContainer = new TestAzureBlobContainer(
                CloudConfiguration.GetStorageAccount("DataConnectionString"),
                AzureBlobTestContainer);

            var objId = Guid.NewGuid().ToString();
            azureBlobContainer.Save(objId, "testText");

            var lockContext = new PessimisticConcurrencyContext()
            {
                ObjectId = objId
            };

            Assert.IsTrue(azureBlobContainer.AcquireLock(lockContext));
            Assert.IsNotNull(lockContext.LockId);
            var firstLockId = lockContext.LockId;

            azureBlobContainer.ReleaseLock(lockContext);

            Assert.IsTrue(azureBlobContainer.AcquireLock(lockContext));
            Assert.IsNotNull(lockContext.LockId);
            Assert.AreNotEqual(firstLockId, lockContext.LockId);
        }

        [TestMethod]
        public void OptimisticCreateNewBlob()
        {
            var azureBlobContainer = new EntitiesBlobContainer<string>(
                CloudConfiguration.GetStorageAccount("DataConnectionString"),
                AzureBlobTestContainer);

            var objId = Guid.NewGuid().ToString();

            OptimisticConcurrencyContext context;
            var text = azureBlobContainer.Get(objId, out context);

            Assert.IsNull(text);
            Assert.AreEqual(context.ObjectId, objId);

            azureBlobContainer.Save(context, "testText");

            text = azureBlobContainer.Get(objId);

            Assert.IsNotNull(text);
            Assert.AreEqual(context.ObjectId, objId);
        }

        [TestMethod]
        [ExpectedException(typeof(StorageException))]
        public void OptimisticCannotCreateSameBlobTwice()
        {
            var azureBlobContainer = new EntitiesBlobContainer<string>(
                CloudConfiguration.GetStorageAccount("DataConnectionString"),
                AzureBlobTestContainer);

            var objId = Guid.NewGuid().ToString();

            OptimisticConcurrencyContext context;
            var text = azureBlobContainer.Get(objId, out context);

            Assert.IsNull(text);
            Assert.AreEqual(context.ObjectId, objId);

            azureBlobContainer.Save(context, "testText");

            text = azureBlobContainer.Get(objId);

            Assert.IsNotNull(text);

            azureBlobContainer.Save(context, "testText");
        }

        [TestMethod]
        [ExpectedException(typeof(StorageException))]//hieu
        public void OptimisticCannotWriteSameBlobTwice()
        {
            var azureBlobContainer = new EntitiesBlobContainer<string>(
                CloudConfiguration.GetStorageAccount("DataConnectionString"),
                AzureBlobTestContainer);

            var objId = Guid.NewGuid().ToString();
            azureBlobContainer.Save(objId, "testText");

            OptimisticConcurrencyContext context;
            var text = azureBlobContainer.Get(objId, out context);

            Assert.IsNotNull(text);
            Assert.AreEqual(context.ObjectId, objId);

            azureBlobContainer.Save(context, "testText");

            text = azureBlobContainer.Get(objId);

            Assert.IsNotNull(text);

            azureBlobContainer.Save(context, "testText");
        }

        private class TestAzureBlobContainer : AzureBlobContainer<string>
        {
            public TestAzureBlobContainer(CloudStorageAccount account, string containerName) : base(account, containerName) { }

            protected override string ReadObject(CloudBlockBlob blob)//hieu
            {
                return new JavaScriptSerializer().Deserialize<string>(blob.DownloadText());
            }

            protected override void WriteOject(CloudBlockBlob blob, BlobRequestOptions options, AccessCondition condition, string obj) //hieu
            {
                blob.UploadText(new JavaScriptSerializer().Serialize(obj), Encoding.Default, condition, options, null);//hieu
            }

            protected override byte[] BinarizeObjectForStreaming(BlobProperties properties, string obj)
            {
                var stream = new MemoryStream();
                using (var writer = new StreamWriter(stream, Encoding.Default))
                {
                    writer.Write(new JavaScriptSerializer().Serialize(obj));
                    writer.Flush();

                    return stream.ToArray();
                }
            }
        }
    }
}