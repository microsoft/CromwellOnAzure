// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.ComponentModel.DataAnnotations;
using System.Linq;
using Microsoft.AspNetCore.Mvc.Controllers;
using Microsoft.OpenApi.Models;
using Swashbuckle.AspNetCore.SwaggerGen;

namespace TesApi.Filters
{
    /// <summary>
    /// Path Parameter Validation Rules Filter
    /// </summary>
    public class GeneratePathParamsValidationFilter : IOperationFilter
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="operation">Operation</param>
        /// <param name="context">OperationFilterContext</param>
        public void Apply(OpenApiOperation operation, OperationFilterContext context)
        {
            var pars = context.ApiDescription.ParameterDescriptions;

            foreach (var par in pars)
            {
                var swaggerParam = operation.Parameters.SingleOrDefault(p => p.Name == par.Name);

                var attributes = ((ControllerParameterDescriptor)par.ParameterDescriptor).ParameterInfo.CustomAttributes;

                if (attributes is not null && attributes.Any() && swaggerParam is not null)
                {
                    // Required - [Required]
                    var requiredAttr = attributes.FirstOrDefault(p => p.AttributeType == typeof(RequiredAttribute));
                    if (requiredAttr is not null)
                    {
                        swaggerParam.Required = true;
                    }

                    // Regex Pattern [RegularExpression]
                    var regexAttr = attributes.FirstOrDefault(p => p.AttributeType == typeof(RegularExpressionAttribute));
                    if (regexAttr is not null)
                    {
                        swaggerParam.Schema.Pattern = (string)regexAttr.ConstructorArguments[0].Value;
                    }

                    // String Length [StringLength]
                    int? minLength = null, maxLength = null;
                    var stringLengthAttr = attributes.FirstOrDefault(p => p.AttributeType == typeof(StringLengthAttribute));
                    if (stringLengthAttr is not null)
                    {
                        if (stringLengthAttr.NamedArguments.Count == 1)
                        {
                            minLength = (int)stringLengthAttr.NamedArguments.Single(p => p.MemberName == "MinimumLength").TypedValue.Value;
                        }
                        maxLength = (int)stringLengthAttr.ConstructorArguments[0].Value;
                    }

                    var minLengthAttr = attributes.FirstOrDefault(p => p.AttributeType == typeof(MinLengthAttribute));
                    if (minLengthAttr is not null)
                    {
                        minLength = (int)minLengthAttr.ConstructorArguments[0].Value;
                    }

                    var maxLengthAttr = attributes.FirstOrDefault(p => p.AttributeType == typeof(MaxLengthAttribute));
                    if (maxLengthAttr is not null)
                    {
                        maxLength = (int)maxLengthAttr.ConstructorArguments[0].Value;
                    }

                    swaggerParam.Schema.MinLength = minLength;
                    swaggerParam.Schema.MaxLength = maxLength;

                    // Range [Range]
                    var rangeAttr = attributes.FirstOrDefault(p => p.AttributeType == typeof(RangeAttribute));
                    if (rangeAttr is not null)
                    {
                        swaggerParam.Schema.Minimum = (int)rangeAttr.ConstructorArguments[0].Value;
                        swaggerParam.Schema.Maximum = (int)rangeAttr.ConstructorArguments[1].Value;
                    }
                }
            }
        }
    }
}

