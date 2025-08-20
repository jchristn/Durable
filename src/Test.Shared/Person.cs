namespace Test.Shared
{
    using System;
    using Durable;

    // Person model
    [Entity("people")]
    public class Person
    {
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        [Property("first", Flags.String, 64)]
        public string FirstName { get; set; }

        [Property("last", Flags.String, 64)]
        public string LastName { get; set; }

        [Property("age")]
        public int Age { get; set; }

        [Property("email", Flags.String, 128)]
        public string Email { get; set; }

        [Property("salary")]
        public decimal Salary { get; set; }

        [Property("department", Flags.String, 32)]
        public string Department { get; set; }

        public string Name => $"{FirstName} {LastName}";

        public override string ToString()
        {
            return $"Person: Id={Id}, Name={FirstName} {LastName}, Age={Age}, Email={Email}, Salary={Salary:C}, Dept={Department}";
        }
    }

}
