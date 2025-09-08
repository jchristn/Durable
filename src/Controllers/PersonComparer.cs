using Test.Shared;

namespace TestApi.Controllers;

public class PersonComparer : IEqualityComparer<Person>
{
    public bool Equals(Person x, Person y)
    {
        if (ReferenceEquals(x, y)) return true;
        if (x is null || y is null) return false;
        return x.Id == y.Id;
    }

    public int GetHashCode(Person obj)
    {
        return obj?.Id.GetHashCode() ?? 0;
    }
}