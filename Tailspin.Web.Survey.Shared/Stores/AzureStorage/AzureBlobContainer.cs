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
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Text;
    using Microsoft.WindowsAzure;
    using Microsoft.WindowsAzure.Storage;
    //using Microsoft.WindowsAzure.Storage;Client.Protocol;
    using Tailspin.Web.Survey.Shared.Helpers;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.Blob.Protocol;
    using Microsoft.WindowsAzure.Storage.Shared.Protocol;

    public abstract class AzureBlobContainer<T> : AzureStorageWithRetryPolicy, IAzureBlobContainer<T>
    {
        protected const int BlobRequestTimeout = 120;

        protected readonly CloudBlobContainer Container;
        protected readonly CloudStorageAccount Account;

        private readonly IDictionary<Type, Action<IConcurrencyControlContext, T>> writingStrategies;

        public AzureBlobContainer(CloudStorageAccount account)
            : this(account, typeof(T).Name.ToLowerInvariant()) { }

        public AzureBlobContainer(CloudStorageAccount account, string containerName)
        {
            this.Account = account;

            var client = account.CreateCloudBlobClient();

            // retry policy is handled by TFHAB
            //client.RetryPolicy = RetryPolicies.NoRetry();//hieu

            this.Container = client.GetContainerReference(containerName);

            this.writingStrategies = new Dictionary<Type, Action<IConcurrencyControlContext, T>>()
            {
                { typeof(OptimisticConcurrencyContext), this.OptimisticControlContextWriteStrategy },
                { typeof(PessimisticConcurrencyContext), this.PessimisticControlContextWriteStrategy }
            };
        }

        public void RegisterConcurrencyWriteStrategy<C>(Action<IConcurrencyControlContext, T> writeAction) where C : IConcurrencyControlContext
        {
            this.writingStrategies[typeof(C)] = writeAction;
        }

        public bool AcquireLock(PessimisticConcurrencyContext lockContext)
        {
            CloudBlobClient client = this.Account.CreateCloudBlobClient();
            CloudBlobContainer container = client.GetContainerReference(this.Container.Name);
            CloudBlockBlob blob = container.GetBlockBlobReference(lockContext.ObjectId);

            //CloudBlockBlob blob = this.Container.GetBlockBlobReference(lockContext.ObjectId);
            TimeSpan? leaseTime = TimeSpan.FromSeconds(BlobRequestTimeout);//Acquire a 15 second lease on the blob. Leave it null for infinite lease. Otherwise it should be between 15 and 60 seconds.
            string proposedLeaseId = Guid.NewGuid().ToString(); //null;//proposed lease id (leave it null for storage service to return you one).

            /*
            //var request = BlobRequest.Lease(this.GetUri(lockContext.ObjectId), BlobRequestTimeout, LeaseAction.Acquire, null);
            //this.Account.Credentials.SignRequest(request);

            // add extra headers not supported by SDK - not supported by emulator yet (SDK 1.7)
            ////request.Headers["x-ms-version"] = "2012-02-12";
            ////request.Headers.Add("x-ms-lease-duration", lockContext.Duration.TotalSeconds.ToString());
            */
            try
            {
                lockContext.LockId = blob.AcquireLease(leaseTime, proposedLeaseId);
                return true;
                /*
                   //using (var response = request.GetResponse())
                //{
                //    if (response is HttpWebResponse &&
                //       HttpStatusCode.Created.Equals((response as HttpWebResponse).StatusCode))
                //    {
                //        lockContext.LockId = response.Headers["x-ms-lease-id"];
                //        return true;
                //    }
                //    else
                //    {
                //        return false;
                //    }
                //}
                 */
            }
            catch (WebException e)
            {
                TraceHelper.TraceWarning("Warning acquiring blob '{0}' lease: {1}", lockContext.ObjectId, e.Message);
                if (WebExceptionStatus.ProtocolError.Equals(e.Status))
                {
                    if (e.Response is HttpWebResponse)
                    {
                        if (HttpStatusCode.NotFound.Equals((e.Response as HttpWebResponse).StatusCode))
                        {
                            lockContext.LockId = null;
                            return true;
                        }
                        else if (HttpStatusCode.Conflict.Equals((e.Response as HttpWebResponse).StatusCode))
                        {
                            lockContext.LockId = null;
                            return false;
                        }
                    }
                    throw;
                }
                return false;
            }
            catch (Exception e)
            {
                TraceHelper.TraceError("Error acquiring blob '{0}' lease: {1}", lockContext.ObjectId, e.Message);
                throw;
            }
        }

        public void ReleaseLock(PessimisticConcurrencyContext lockContext)
        {
            if (string.IsNullOrWhiteSpace(lockContext.LockId))
            {
                throw new ArgumentNullException("lockContext.LockId", "LockId cannot be null or empty");
            }
            CloudBlockBlob blob = this.Container.GetBlockBlobReference(lockContext.ObjectId);
            blob.ReleaseLease(AccessCondition.GenerateEmptyCondition());            
            /*
            //var request = BlobRequest.Lease(this.GetUri(lockContext.ObjectId), BlobRequestTimeout, LeaseAction.Release, lockContext.LockId);
            //this.Account.Credentials.SignRequest(request);
           
            //using (var response = request.GetResponse())
            //{
            //    if (response is HttpWebResponse &&
            //        !HttpStatusCode.OK.Equals((response as HttpWebResponse).StatusCode))
            //    {
            //        TraceHelper.TraceError("Error releasing blob '{0}' lease: {1}", lockContext.ObjectId, (response as HttpWebResponse).StatusDescription);
            //        throw new InvalidOperationException((response as HttpWebResponse).StatusDescription);
            //    }
            //}
             */
        }

        public virtual void Delete(string objId)
        {
            this.StorageRetryPolicy.ExecuteAction(() =>
            {
                var blob = this.Container.GetBlobReferenceFromServer(objId);
                blob.DeleteIfExists();
            });
        }

        public virtual void DeleteContainer()
        {
            this.StorageRetryPolicy.ExecuteAction(() =>
            {
                try
                {
                    this.Container.Delete();
                }
                catch (StorageException ex) //hieu
                {
                    var requestInformation = ex.RequestInformation;
                    //var errorCode = requestInformation.ExtendedErrorInformation.ErrorCode;//errorCode = ContainerAlreadyExists
                    var statusCode = (System.Net.HttpStatusCode)requestInformation.HttpStatusCode;//request
                    if (statusCode == HttpStatusCode.NotFound)
                    {
                        TraceHelper.TraceWarning(ex.TraceInformation());
                        return;
                    }

                    TraceHelper.TraceError(ex.TraceInformation());

                    throw;
                }
            });
        }

        public virtual void EnsureExist()
        {
            this.StorageRetryPolicy.ExecuteAction(() => this.Container.CreateIfNotExists());
        }

        public virtual T Get(string objId)
        {
            OptimisticConcurrencyContext optimisticContext;
            return this.Get(objId, out optimisticContext);
        }

        public virtual T Get(string objId, out OptimisticConcurrencyContext context)
        {
            OptimisticConcurrencyContext optimisticContext = null;
            var result = this.StorageRetryPolicy.ExecuteAction<T>(() =>
                {
                    try
                    {
                        var blob = this.Container.GetBlockBlobReference(objId);
                        blob.FetchAttributes();
                        optimisticContext = new OptimisticConcurrencyContext(blob.Properties.ETag) { ObjectId = objId };
                        return this.ReadObject(blob);
                    }
                    catch (StorageException ex)
                    {
                        var requestInformation = ex.RequestInformation;
                        //var errorCode = requestInformation.ExtendedErrorInformation.ErrorCode;//errorCode = ContainerAlreadyExists
                        var statusCode = (System.Net.HttpStatusCode)requestInformation.HttpStatusCode;//request

                        TraceHelper.TraceWarning(ex.TraceInformation());
                        if (HttpStatusCode.NotFound.Equals(statusCode) )
                        //if (HttpStatusCode.NotFound.Equals(statusCode) &&
                        //    (BlobErrorCodeStrings.BlobNotFound.Equals(errorCode) ||
                        //    StorageErrorCodeStrings.ResourceNotFound.Equals(errorCode)))
                        {
                            optimisticContext = this.GetContextForUnexistentBlob(objId);
                            return default(T);
                        }
                        throw;
                    }
                });
            context = optimisticContext;
            return result;
        }

        public virtual IEnumerable<IListBlobItemWithName> GetBlobList()
        {
            return this.StorageRetryPolicy.ExecuteAction<IEnumerable<IListBlobItemWithName>>(() => this.Container.ListBlobs().Select(b => new AzureBlob(b as CloudBlockBlob)));
        }

        public virtual Uri GetUri(string objId)
        {
            CloudBlockBlob blob = this.Container.GetBlockBlobReference(objId);
            return blob.Uri;
        }

        public virtual void Save(string objId, T obj)
        {
            var context = new OptimisticConcurrencyContext() { ObjectId = objId };
            this.Save(context, obj);
        }

        public virtual void Save(IConcurrencyControlContext context, T obj)
        {
            if (string.IsNullOrWhiteSpace(context.ObjectId))
            {
                throw new ArgumentNullException("context.ObjectId", "ObjectId cannot be null or empty");
            }

            Action<IConcurrencyControlContext, T> writeStrategy;
            if (!this.writingStrategies.TryGetValue(context.GetType(), out writeStrategy))
            {
                throw new InvalidOperationException("IConcurrencyControlContext implementation not registered");
            }

            this.StorageRetryPolicy.ExecuteAction(() => writeStrategy(context, obj));
        }

        protected virtual OptimisticConcurrencyContext GetContextForUnexistentBlob(string objId)
        {
            return new OptimisticConcurrencyContext()
            {
                ObjectId = objId,
                AccessCondition = AccessCondition.GenerateIfModifiedSinceCondition(DateTime.Now)//hieu not datetime.minvalue
            };
        }

        protected virtual void OptimisticControlContextWriteStrategy(IConcurrencyControlContext context, T obj)
        {
            CloudBlockBlob blob = this.Container.GetBlockBlobReference(context.ObjectId);
            
            //hieu
            //var blobRequestOptions = new BlobRequestOptions()
            //{
            //    AccessCondition = (context as OptimisticConcurrencyContext).AccessCondition
            //};

            this.WriteOject(blob, null, obj);
        }

        protected virtual void PessimisticControlContextWriteStrategy(IConcurrencyControlContext context, T obj)
        {
            if (string.IsNullOrWhiteSpace((context as PessimisticConcurrencyContext).LockId))
            {
                throw new ArgumentNullException("context.LockId", "LockId cannot be null or empty");
            }

            var blobProperties = new BlobProperties();

            var binarizedObject = this.BinarizeObjectForStreaming(blobProperties, obj);

            CloudBlockBlob blob = this.Container.GetBlockBlobReference(context.ObjectId);

            MemoryStream memoryBuffer = new MemoryStream();
            blob.PutBlock(context.ObjectId, memoryBuffer, null);

            //hieu
            //var updateRequest = BlobRequest.Put(
            //    this.GetUri(context.ObjectId),
            //    BlobRequestTimeout,
            //    blobProperties,
            //    BlobType.BlockBlob,
            //    (context as PessimisticConcurrencyContext).LockId,
            //    0);

            using (var writer = new BinaryWriter(memoryBuffer, Encoding.Default))
            {
                writer.Write(binarizedObject);
            }
            //hieu
            //this.Account.Credentials.SignRequest(updateRequest);

            //using (var response = updateRequest.GetResponse())
            //{
            //    if (response is HttpWebResponse &&
            //        !HttpStatusCode.Created.Equals((response as HttpWebResponse).StatusCode))
            //    {
            //        TraceHelper.TraceError("Error writing leased blob '{0}': {1}", context.ObjectId, (response as HttpWebResponse).StatusDescription);
            //        throw new InvalidOperationException((response as HttpWebResponse).StatusDescription);
            //    }
            //}
        }

        protected abstract T ReadObject(CloudBlockBlob blob);

        protected abstract void WriteOject(CloudBlockBlob blob, BlobRequestOptions options, T obj);

        protected abstract byte[] BinarizeObjectForStreaming(BlobProperties properties, T obj);
    }
}