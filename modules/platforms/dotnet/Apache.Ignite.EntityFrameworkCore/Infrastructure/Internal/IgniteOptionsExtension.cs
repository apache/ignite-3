// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

namespace Apache.Ignite.EntityFrameworkCore.Infrastructure.Internal;

using System.Collections.Generic;
using System.Text;
using Extensions;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

public class IgniteOptionsExtension : RelationalOptionsExtension
{
    private DbContextOptionsExtensionInfo? _info;

    public IgniteOptionsExtension()
    {
    }

    protected IgniteOptionsExtension(IgniteOptionsExtension copyFrom)
        : base(copyFrom)
    {
    }

    public override DbContextOptionsExtensionInfo Info
        => _info ??= new ExtensionInfo(this);

    protected override RelationalOptionsExtension Clone()
        => new IgniteOptionsExtension(this);

    public override void ApplyServices(IServiceCollection services)
        => services.AddEntityFrameworkIgnite();

    private sealed class ExtensionInfo : RelationalExtensionInfo
    {
        private string? _logFragment;

        public ExtensionInfo(IDbContextOptionsExtension extension)
            : base(extension)
        {
        }

        private new IgniteOptionsExtension Extension
            => (IgniteOptionsExtension)base.Extension;

        public override bool IsDatabaseProvider
            => true;

        public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other)
            => other is ExtensionInfo;

        public override string LogFragment
        {
            get
            {
                if (_logFragment == null)
                {
                    var builder = new StringBuilder();

                    builder.Append(base.LogFragment);

                    _logFragment = builder.ToString();
                }

                return _logFragment;
            }
        }

        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo) => debugInfo["Ignite"] = "1";
    }
}
