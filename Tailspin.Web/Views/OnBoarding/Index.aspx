<%--
===============================================================================
 Microsoft patterns & practices
 Windows Azure Architecture Guide
===============================================================================
 Copyright © Microsoft Corporation.  All rights reserved.
 This code released under the terms of the 
 Microsoft patterns & practices license (http://wag.codeplex.com/license)
===============================================================================
--%>

<%@ Page Language="C#" MasterPageFile="~/Views/Shared/Site.Master" Inherits="System.Web.Mvc.ViewPage<Tailspin.Web.Models.TenantPageViewData<IEnumerable<Tailspin.Web.Survey.Shared.Models.Tenant>>>" %>

<asp:Content ID="Content1" ContentPlaceHolderID="MenuContent" runat="server">
    <div class="clear">
    </div>
</asp:Content>

<asp:Content ID="Content2" ContentPlaceHolderID="MainContent" runat="server">
    <h2>
        Welcome to Tailspin!
    </h2>
    <br />
    <%= Html.ActionLink("Enroll your company", "Join") %> in Tailspin!
    <p>
        To test the application, some tenants have already been provisioned:</p>
    <div id="configured-tenants">
        <ul class="tenants-list">
            <%foreach (var tenant in this.Model.ContentModel)
              { %>
            <li>
                <div class="configured-tenant-logo">
                    <a href="<%:Url.Action("Index", "Surveys", new { area = "Survey", tenant = tenant.Name.ToLowerInvariant() }, null)%>"
                        class="configured-tenants-links">
                        <%if (string.IsNullOrWhiteSpace(tenant.Logo))
                          { %>   
                            <img src="../../Content/img/tenant-nologo.png" alt="<%=tenant.Name%>" />
                        <%}
                          else
                          { %>
                            <img src="<%=tenant.Logo%>" alt="<%=tenant.Name%> logo" />
                        <%} %>
                    </a>
                </div>
                <div class="configured-tenant-description">
                    <%if (string.IsNullOrWhiteSpace(tenant.WelcomeText))
                      { %>
                        Select the image to sign in as the <b><%=tenant.Name%> administrator</b>
                    <%}
                      else
                      { %>
                        <%=tenant.WelcomeText%>
                    <%} %>
                </div>
            </li>
            <% } %>
        </ul>
    </div>
</asp:Content>
