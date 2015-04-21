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
    public enum UDFType
    {
        Bool,
        Guid,
        Integer,
        Long,
        Double,
        String
    }

    public class UDFMetadata
    {
        private UDFType type;

        public string Name { get; set; }

        public UDFType Type
        {
            get
            {
                return this.type;
            }
            set
            {
                this.type = value;
                this.OnTypeChanged();
            }
        }

        public string Display { get; set; }

        public bool Mandatory { get; set; }

        public string DefaultValue { get; set; }

        public virtual void CopyFrom(UDFMetadata item)
        {
            this.Name = item.Name;
            this.Type = item.Type;
            this.Display = item.Display;
            this.Mandatory = item.Mandatory;
        }

        public string GetEdmType()
        {
            switch (this.Type)
            {
                case UDFType.Bool:
                    return "Edm.Boolean";
                case UDFType.Double:
                    return "Edm.Double";
                case UDFType.Guid:
                    return "Edm.Guid";
                case UDFType.Integer:
                    return "Edm.Int32";
                case UDFType.Long:
                    return "Edm.Int64";
                default:
                    return "Edm.String";
            }
        }

        public void SetUDFType(string entityType)
        {
            switch (entityType)
            {
                case "Edm.Boolean":
                    this.Type = UDFType.Bool;
                    break;
                case "Edm.Double":
                    this.Type = UDFType.Double;
                    break;
                case "Edm.Guid":
                    this.Type = UDFType.Guid;
                    break;
                case "Edm.Int32":
                    this.Type = UDFType.Integer;
                    break;
                case "Edm.Int64":
                    this.Type = UDFType.Long;
                    break;
                default:
                    this.Type = UDFType.String;
                    break;
            }
        }

        protected virtual void OnTypeChanged() { }
    }
}
