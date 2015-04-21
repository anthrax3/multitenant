//===============================================================================
// Microsoft patterns & practices
// Windows Azure Architecture Guide
//===============================================================================
// Copyright © Microsoft Corporation.  All rights reserved.
// This code released under the terms of the 
// Microsoft patterns & practices license (http://wag.codeplex.com/license)
//===============================================================================


[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("BEST.Security.Config", "ASPCONFIG:DenyAnonymous", Justification = "Because we are using WIF / Claims we must have anonymous access turned on so the flow can work.")]
[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.WebConfigurationSecurity", "CA3128:MachineKeyValidationKeyRule", Justification = "Need this to support Azure. Need the same key for each machine to support validation token.")]
[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.WebConfigurationSecurity", "CA3126:MachineKeyDecryptionKeyRule", Justification = "Need this to support Azure. Need the same key for each machine to support validation token.")]