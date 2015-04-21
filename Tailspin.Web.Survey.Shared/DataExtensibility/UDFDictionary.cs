//===============================================================================
// Microsoft patterns & practices
// Windows Azure Architecture Guide
//===============================================================================
// Copyright © Microsoft Corporation.  All rights reserved.
// This code released under the terms of the 
// Microsoft patterns & practices license (http://wag.codeplex.com/license)
//===============================================================================


namespace Tailspin.Web.Survey.Shared.DataExtensibility
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text.RegularExpressions;
    using Tailspin.Web.Survey.Shared.Models;
    using Tailspin.Web.Survey.Shared.Stores;

    public class UDFDictionary : IUDFDictionary
    {
        private readonly ITenantStore tenantStore;
        private readonly Regex fieldNamePattern;

        public UDFDictionary(ITenantStore tenantStore)
        {
            this.tenantStore = tenantStore;
            this.fieldNamePattern = new Regex(@"^[a-zA-z]\w{2}\w*$", RegexOptions.Compiled);
        }

        public IEnumerable<UDFMetadata> GetFieldsFor<T>(string tenant) where T : IUDFModel
        {
            return this.GetFieldsFor(tenant, typeof(T));
        }

        public IEnumerable<UDFMetadata> GetFieldsFor(string tenant, Type modelType)
        {
            if (modelType.GetInterface("IUDFModel") == null)
            {
                throw new InvalidOperationException("udfModelType should implement IUDFModel interface");
            }

            var tenantData = this.tenantStore.GetTenant(tenant);
            if (tenantData != null
                && tenantData.SubscriptionKind.Equals(SubscriptionKind.Premium)
                && tenantData.ExtensionDictionary != null)
            {
                List<UDFMetadata> definitions = null;
                if (tenantData.ExtensionDictionary.TryGetValue(modelType.Name, out definitions))
                {
                    return definitions;
                }
            }

            return null;
        }

        public bool AddFieldFor<T>(string tenant, UDFMetadata metadata, out IEnumerable<UDFMetadataError> errorMessages) where T : IUDFModel
        {
            return this.AddFieldFor(tenant, typeof(T), metadata, out errorMessages);
        }

        public bool AddFieldFor(string tenant, Type modelType, UDFMetadata metadata, out IEnumerable<UDFMetadataError> errorMessages)
        {
            if (modelType.GetInterface("IUDFModel") == null)
            {
                throw new InvalidOperationException("udfModelType should implement IUDFModel interface");
            }

            if (!this.IsValid(metadata, out errorMessages))
            {
                return false;
            }

            var tenantData = this.tenantStore.GetTenant(tenant);

            if (tenantData.ExtensionDictionary == null)
            {
                tenantData.ExtensionDictionary = new Dictionary<string, List<UDFMetadata>>();
            }

            List<UDFMetadata> udfs;
            if (!tenantData.ExtensionDictionary.TryGetValue(modelType.Name, out udfs))
            {
                tenantData.ExtensionDictionary.Add(modelType.Name, new List<UDFMetadata>() { metadata });
            }
            else
            {
                if (udfs.FirstOrDefault(i => i.Name.Equals(metadata.Name, StringComparison.InvariantCultureIgnoreCase)) != null)
                {
                    errorMessages = new List<UDFMetadataError>() 
                    {
                        new UDFMetadataError()
                        {
                            Field = UDFMetadataField.Name,
                            Message = "* Field name already used in this model" 
                        } 
                    };
                    return false;
                }
                else
                {
                    udfs.Add(metadata);
                }
            }

            this.tenantStore.SaveTenant(tenantData);

            errorMessages = null;
            return true;
        }

        public void RemoveFieldFor<T>(string tenant, string fieldName) where T : IUDFModel
        {
            this.RemoveFieldFor(tenant, typeof(T), fieldName);
        }

        public void RemoveFieldFor(string tenant, Type modelType, string fieldName)
        {
            List<UDFMetadata> udfs;

            var tenantData = this.tenantStore.GetTenant(tenant);
            if (tenantData.ExtensionDictionary != null && tenantData.ExtensionDictionary.TryGetValue(modelType.Name, out udfs))
            {
                udfs.RemoveAll(i => i.Name.Equals(fieldName, StringComparison.InvariantCultureIgnoreCase));
                this.tenantStore.SaveTenant(tenantData);
            }
        }

        public IList<UDFItem> InstanceFieldsFor<T>(string tenant) where T : IUDFModel
        {
            var udfs = this.GetFieldsFor<T>(tenant);
            if (udfs != null)
            {
                return udfs.Select(m => new UDFItem(m)).ToList();
            }

            return null;
        }

        public bool AreValidFor<T>(string tenant, IList<UDFItem> list) where T : IUDFModel
        {
            var current = this.InstanceFieldsFor<T>(tenant);
            return (current == null && list == null) ||
                ((current != null && list != null) && current.Count.Equals(list.Count));
        }

        private bool IsValid(UDFMetadata metadata, out IEnumerable<UDFMetadataError> errorMessages)
        {
            var errors = new List<UDFMetadataError>();

            if (string.IsNullOrWhiteSpace(metadata.Name) || metadata.Name.Equals("new", StringComparison.InvariantCultureIgnoreCase))
            {
                errors.Add(new UDFMetadataError()
                {
                    Field = UDFMetadataField.Name,
                    Message = "Cannot be empty or 'new'"
                });
            }
            else if (!this.fieldNamePattern.IsMatch(metadata.Name))
            {
                errors.Add(new UDFMetadataError()
                {
                    Field = UDFMetadataField.Name,
                    Message = "Not allowed (must start with a letter and len >= 3)"
                });
            }

            if (string.IsNullOrWhiteSpace(metadata.Display))
            {
                errors.Add(new UDFMetadataError()
                {
                    Field = UDFMetadataField.Display,
                    Message = "Cannot be empty"
                });
            }

            errorMessages = errors;
            return errors.Count == 0;
        }
    }
}
