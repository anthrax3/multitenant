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

    public interface IUDFDictionary
    {
        IEnumerable<UDFMetadata> GetFieldsFor<T>(string tenant) where T : IUDFModel;
        IEnumerable<UDFMetadata> GetFieldsFor(string tenant, Type modelType);
        bool AddFieldFor<T>(string tenant, UDFMetadata metadata, out IEnumerable<UDFMetadataError> errorMessages) where T : IUDFModel;
        bool AddFieldFor(string tenant, Type modelType, UDFMetadata metadata, out IEnumerable<UDFMetadataError> errorMessages);
        void RemoveFieldFor<T>(string tenant, string fieldName) where T : IUDFModel;
        void RemoveFieldFor(string tenant, Type modelType, string fieldName);
        IList<UDFItem> InstanceFieldsFor<T>(string tenant) where T : IUDFModel;
        bool AreValidFor<T>(string tenant, IList<UDFItem> list) where T : IUDFModel;
    }
}
