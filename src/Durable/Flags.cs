namespace Durable
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Text;
    using Microsoft.Data.Sqlite;

    [Flags]
    public enum Flags
    {
        None = 0,
        PrimaryKey = 1,
        String = 2,
        AutoIncrement = 4
    }
}