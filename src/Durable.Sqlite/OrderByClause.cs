namespace Durable.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    internal class OrderByClause
    {
        public string Column { get; set; }
        public bool Ascending { get; set; }
    }
}
