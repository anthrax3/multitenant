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

<%@ Page Language="C#" MasterPageFile="~/Views/Shared/Site.Master" Inherits="System.Web.Mvc.ViewPage<Tailspin.Web.Models.TenantPageViewData<Tailspin.Web.Survey.Shared.Models.Survey>>" %>

<%@ Import Namespace="Tailspin.Web.Utility" %>

<asp:Content ID="Content1" ContentPlaceHolderID="MenuContent" runat="server">
    <ul>
        <li class="current"><a>New Survey</a></li>
        <li><%:Html.ActionLink("My Surveys", "Index", "Surveys")%></li>
        <li><%:Html.ActionLink("My Account", "Index", "Account", new {area = string.Empty}, null)%></li>
        <%if (Tailspin.Web.Survey.Shared.Models.SubscriptionKind.Premium.Equals(this.Model.Tenant.SubscriptionKind))
          { %>
          <li><%:Html.ActionLink("Model extensions", "ModelIndex", "Account", new { area = string.Empty }, null)%></li>
        <%} %>
    </ul>
    <div class="clear">
    </div>
</asp:Content>

<asp:Content ID="Content2" ContentPlaceHolderID="MainContent" runat="server">
    <div class="breadcrumbs">
        New Survey
    </div>
    <h2>
        Create a new survey</h2>
    <br />
    <% using (Html.BeginForm())
       { %>
    <%: Html.ValidationSummary(true) %>
    <%: Html.AntiForgeryToken() %>
    <dl>
        <dt>
            <%: Html.LabelFor(model => model.ContentModel.Title) %>
        </dt>
        <dd>
            <%: Html.TextBoxFor(model => model.ContentModel.Title, new { size = "45" })%>
            <%: Html.ValidationMessageFor(model => model.ContentModel.Title)%>
        </dd>
        <%if (Model.ContentModel.UserDefinedFields != null)
          {
              for (int i = 0; i < this.Model.ContentModel.UserDefinedFields.Count; i++)
              {%>
        <dt>
            <%: this.Model.ContentModel.UserDefinedFields[i].Display%>
        </dt>
        <dd>
            <%: Html.TextBoxFor(m => Model.ContentModel.UserDefinedFields[i].Value, new { size = "45" })%>
            <%: Html.ValidationMessageFor(model => model.ContentModel.UserDefinedFields[i].Value)%>
        </dd>
        <%}%>
        <%: Html.ValidationMessageFor(model => model.ContentModel.UserDefinedFields)%>
        <%}       
        %>
    </dl>
    <br />
    <table>
        <tr>
            <th align="left">
                Questions
            </th>
        </tr>
        <tr>
            <td>
                <a href="#" onclick="javascript: submitToNewQuestion()">Add Question</a>
            </td>
            <td>
                <%: Html.ValidationMessageFor(model => model.ContentModel.Questions) %>            
            </td>
        </tr>
        <% foreach (var question in this.Model.ContentModel.Questions)
           { %>
        <tr>
            <td>
                <%:question.Text%>
            </td>
        </tr>
        <% } %>
    </table>
    <br />
    <br />
    <p>
        <input id="create" type="submit" value="Create" class="bigOrangeButton" />
    </p>
    <% } %>

    <script type="text/javascript">
        function submitToNewQuestion() {
            document.forms[0].action = '<%=Url.Action("NewQuestion", "Surveys")%>'
            document.forms[0].submit();
        }
    </script>
</asp:Content>
