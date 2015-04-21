//===============================================================================
// Microsoft patterns & practices
// Windows Azure Architecture Guide
//===============================================================================
// Copyright © Microsoft Corporation.  All rights reserved.
// This code released under the terms of the 
// Microsoft patterns & practices license (http://wag.codeplex.com/license)
//===============================================================================


namespace Tailspin.Web.Survey.Shared.Tests.DataExtensibility
{
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;
    using Tailspin.Web.Survey.Shared.DataExtensibility;
    using Tailspin.Web.Survey.Shared.Models;
    using Tailspin.Web.Survey.Shared.Stores;

    [TestClass]
    public class UDFDictionaryFixture
    {
        [TestMethod]
        public void AddFieldsForReturnsTrue()
        {
            // Setup
            const string Tenant = "tenant";
            var mockTenant = new Mock<Tenant>();
            var metadata = new UDFMetadata { Name = "custom", Display = "display" };
            var mockTenantStore = new Mock<ITenantStore>();
            mockTenantStore.Setup(m => m.GetTenant(Tenant)).Returns(mockTenant.Object);
            
            // Act
            var udfDictionary = new UDFDictionary(mockTenantStore.Object);
            IEnumerable<UDFMetadataError> errors = new List<UDFMetadataError>();

            var result = udfDictionary.AddFieldFor<TestExtension>(Tenant, metadata, out errors);

            // Assert
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void AddFieldsForReturnsErrorWhenRequiredFieldIsMissing()
        {
            // Setup
            const string Tenant = "tenant";
            var mockTenant = new Mock<Tenant>();
            var metadata = new UDFMetadata { Name = "custom" };
            var mockTenantStore = new Mock<ITenantStore>();
            mockTenantStore.Setup(m => m.GetTenant(Tenant)).Returns(mockTenant.Object);

            // Act
            var udfDictionary = new UDFDictionary(mockTenantStore.Object);
            IEnumerable<UDFMetadataError> errors = new List<UDFMetadataError>();

            var result = udfDictionary.AddFieldFor<TestExtension>(Tenant, metadata, out errors);

            // Assert
            Assert.IsFalse(result);
            Assert.IsTrue(errors.Count() > 0);
        }

        [TestMethod]
        public void AddFieldsForReturnsErrorWhenDuplicateFieldIsAdded()
        {
            // Setup
            const string Tenant = "tenant";
            var mockTenant = new Mock<Tenant>();
            var metadata = new UDFMetadata { Name = "custom", Display = "custom" };
            var duplicateMetadata = new UDFMetadata { Name = "custom", Display = "custom" };
            var mockTenantStore = new Mock<ITenantStore>();
            mockTenantStore.Setup(m => m.GetTenant(Tenant)).Returns(mockTenant.Object);

            IEnumerable<UDFMetadataError> errors = new List<UDFMetadataError>();
            var udfDictionary = new UDFDictionary(mockTenantStore.Object);
            udfDictionary.AddFieldFor<TestExtension>(Tenant, metadata, out errors);

            // Act
            var result = udfDictionary.AddFieldFor<TestExtension>(Tenant, duplicateMetadata, out errors);

            // Assert
            Assert.IsFalse(result);
            Assert.IsTrue(errors.Count() > 0);
            Assert.IsTrue(errors.ToList()[0].Field == UDFMetadataField.Name);
        }

        [TestMethod]
        public void AddFieldsForReturnsErrorWhenEmptyFieldNameIsAdded()
        {
            // Setup
            const string Tenant = "tenant";
            var mockTenant = new Mock<Tenant>();
            var metadata = new UDFMetadata { Name = string.Empty, Display = "custom" };
            var mockTenantStore = new Mock<ITenantStore>();
            mockTenantStore.Setup(m => m.GetTenant(Tenant)).Returns(mockTenant.Object);

            IEnumerable<UDFMetadataError> errors = new List<UDFMetadataError>();
            var udfDictionary = new UDFDictionary(mockTenantStore.Object);

            // Act
            var result = udfDictionary.AddFieldFor<TestExtension>(Tenant, metadata, out errors);

            // Assert
            Assert.IsFalse(result);
            Assert.IsTrue(errors.Count() > 0);
            Assert.AreEqual(UDFMetadataField.Name, errors.ToList()[0].Field);
            Assert.AreEqual("Cannot be empty or 'new'", errors.ToList()[0].Message);
        }

        [TestMethod]
        public void AddFieldsForReturnsErrorWhenEmptyFieldDisplayIsAdded()
        {
            // Setup
            const string Tenant = "tenant";
            var mockTenant = new Mock<Tenant>();
            var metadata = new UDFMetadata { Name = "name", Display = string.Empty };
            var mockTenantStore = new Mock<ITenantStore>();
            mockTenantStore.Setup(m => m.GetTenant(Tenant)).Returns(mockTenant.Object);

            IEnumerable<UDFMetadataError> errors = new List<UDFMetadataError>();
            var udfDictionary = new UDFDictionary(mockTenantStore.Object);

            // Act
            var result = udfDictionary.AddFieldFor<TestExtension>(Tenant, metadata, out errors);

            // Assert
            Assert.IsFalse(result);
            Assert.IsTrue(errors.Count() > 0);
            Assert.AreEqual(UDFMetadataField.Display, errors.ToList()[0].Field);
            Assert.AreEqual("Cannot be empty", errors.ToList()[0].Message);
        }

        [TestMethod]
        public void RemoveFieldForRemovesCustomField()
        {
            // Setup
            const string Tenant = "tenant";

            var mockTenant = new Tenant { Name = Tenant, SubscriptionKind = Shared.Models.SubscriptionKind.Premium }; 

            var metadata = new UDFMetadata { Name = "custom", Display = "display" };
            var mockTenantStore = new Mock<ITenantStore>();
            mockTenantStore.Setup(m => m.GetTenant(Tenant)).Returns(mockTenant);

            var udfDictionary = new UDFDictionary(mockTenantStore.Object);
            IEnumerable<UDFMetadataError> errors = new List<UDFMetadataError>();
            var result = udfDictionary.AddFieldFor<TestExtension>(Tenant, metadata, out errors);
            var f1 = udfDictionary.GetFieldsFor<TestExtension>(Tenant);
            
            // Act
            udfDictionary.RemoveFieldFor<TestExtension>(Tenant, "custom");
            var fields = udfDictionary.GetFieldsFor<TestExtension>(Tenant);

            // Assert
            mockTenantStore.Verify(m => m.SaveTenant(mockTenant));
            Assert.AreEqual(0, fields.Count());
        }

        [TestMethod]
        public void GetFieldsForReturnsNullWhenEmptyList()
        {
            // Setup
            const string Tenant = "tenant";

            var mockTenant = new Tenant { Name = Tenant, SubscriptionKind = Shared.Models.SubscriptionKind.Premium };

            var metadata = new UDFMetadata { Name = "custom", Display = "display" };
            var mockTenantStore = new Mock<ITenantStore>();
            mockTenantStore.Setup(m => m.GetTenant(Tenant)).Returns(mockTenant);

            var udfDictionary = new UDFDictionary(mockTenantStore.Object);

            // Act
            var fields = udfDictionary.GetFieldsFor<TestExtension>(Tenant);

            // Assert
            Assert.IsNull(fields);
        }

        [TestMethod]
        public void GetFieldsForReturnsOneWhenFieldIsAdded()
        {
            // Setup
            const string Tenant = "tenant";

            var mockTenant = new Tenant { Name = Tenant, SubscriptionKind = Shared.Models.SubscriptionKind.Premium };

            var metadata = new UDFMetadata { Name = "custom", Display = "display" };
            var mockTenantStore = new Mock<ITenantStore>();
            mockTenantStore.Setup(m => m.GetTenant(Tenant)).Returns(mockTenant);

            var udfDictionary = new UDFDictionary(mockTenantStore.Object);
            IEnumerable<UDFMetadataError> errors = new List<UDFMetadataError>();
            var result = udfDictionary.AddFieldFor<TestExtension>(Tenant, metadata, out errors);

            // Act
            var fields = udfDictionary.GetFieldsFor<TestExtension>(Tenant);

            // Assert
            Assert.AreEqual(1, fields.Count());
        }

        [TestMethod]
        public void AreValidForReturnsTrueWhenExtensionListMatchesModelList()
        {
            // Setup
            const string Tenant = "tenant";
            var mockTenant = new Tenant { Name = Tenant, SubscriptionKind = Shared.Models.SubscriptionKind.Premium };

            var list = new List<UDFItem> { new UDFItem { DefaultValue = "defaultvalue", Name = "name", Value = "value", Display = "display" } };
            var testModel = new TestExtension();

            var mockTenantStore = new Mock<ITenantStore>();
            mockTenantStore.Setup(m => m.GetTenant(Tenant)).Returns(mockTenant);

            var metadata = new UDFMetadata { Name = "custom", Display = "display" };

            var udfDictionary = new UDFDictionary(mockTenantStore.Object);
            IEnumerable<UDFMetadataError> errors = new List<UDFMetadataError>();
            udfDictionary.AddFieldFor<TestExtension>(Tenant, metadata, out errors);

            // Act
            var result = udfDictionary.AreValidFor<TestExtension>(Tenant, list);
            
            // Assert
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void AreValidForReturnsFalseWhenExtensionListDoesNotMatchModelList()
        {
            // Setup
            const string Tenant = "tenant";
            var mockTenant = new Tenant { Name = Tenant, SubscriptionKind = Shared.Models.SubscriptionKind.Premium };

            var list = new List<UDFItem> { new UDFItem { DefaultValue = "defaultvalue", Name = "name", Value = "value", Display = "display" } };
            var testModel = new TestExtension();

            var mockTenantStore = new Mock<ITenantStore>();
            mockTenantStore.Setup(m => m.GetTenant(Tenant)).Returns(mockTenant);

            var udfDictionary = new UDFDictionary(mockTenantStore.Object);

            // Act
            var result = udfDictionary.AreValidFor<TestExtension>(Tenant, list);

            // Assert
            Assert.IsFalse(result);
        }
    }
}
