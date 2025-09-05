using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.Sqlite;
using Durable;
using Test.Shared;

namespace TestApi.Controllers;

[ApiController]
[Route("api/[controller]")]
public class PersonController : ControllerBase
{
    private readonly IRepository<Person> _personRepo;

    public PersonController(IRepository<Person> personRepo)
    {
        _personRepo = personRepo;
    }

    [HttpGet]
    public async Task<IActionResult> GetPeople()
    {
        try
        {
            var people = await _personRepo.ReadAllAsync().ToListAsync();
            return Ok(people);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to retrieve people", details = ex.Message });
        }
    }

    [HttpGet("{id}")]
    public async Task<IActionResult> GetPerson(int id)
    {
        if (id <= 0)
            return BadRequest("Invalid person ID");

        try
        {
            var person = await _personRepo.ReadByIdAsync(id);
            if (person == null)
                return NotFound($"Person with ID {id} not found");
            return Ok(person);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to retrieve person", details = ex.Message });
        }
    }

    [HttpGet("where")]
    public async Task<IActionResult> GetPeopleWhere([FromQuery] string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            return BadRequest("Name parameter is required and cannot be empty");

        try
        {
            var people = await _personRepo.ReadManyAsync(p => p.FirstName.Contains(name) || p.LastName.Contains(name)).ToListAsync();
            return Ok(people);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to search people", details = ex.Message });
        }
    }

    [HttpGet("by-department")]
    public async Task<IActionResult> GetPeopleByDepartment([FromQuery] string department)
    {
        if (string.IsNullOrWhiteSpace(department))
            return BadRequest("Department parameter is required and cannot be empty");

        try
        {
            var people = await _personRepo.ReadManyAsync(p => p.Department == department).ToListAsync();
            return Ok(people);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to retrieve people by department", details = ex.Message });
        }
    }

    [HttpGet("paged")]
    public async Task<IActionResult> GetPeoplePaged([FromQuery] int skip = 0, [FromQuery] int take = 10)
    {
        if (skip < 0)
            return BadRequest("Skip parameter must be non-negative");
        if (take < 1 || take > 100)
            return BadRequest("Take parameter must be between 1 and 100");

        try
        {
            var allPeople = await _personRepo.ReadAllAsync()
                .OrderBy(p => p.LastName)
                .ThenBy(p => p.FirstName)
                .Skip(skip)
                .Take(take)
                .ToListAsync();
            return Ok(allPeople);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to retrieve paged people", details = ex.Message });
        }
    }

    [HttpPost]
    public async Task<IActionResult> CreatePerson([FromBody] Person person)
    {
        if (person == null)
            return BadRequest("Person data is required");

        if (!ModelState.IsValid)
        {
            var errors = ModelState
                .Where(x => x.Value.Errors.Count > 0)
                .ToDictionary(
                    kvp => kvp.Key,
                    kvp => kvp.Value.Errors.Select(e => e.ErrorMessage).ToArray()
                );

            return BadRequest(new
            {
                error = "Validation failed",
                details = "One or more validation errors occurred",
                validationErrors = errors
            });
        }

        try
        {
            var created = await _personRepo.CreateAsync(person);
            return Ok(created);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to create person", details = ex.Message });
        }
    }

    [HttpPut("{id}")]
    public async Task<IActionResult> UpdatePerson(int id, [FromBody] Person person)
    {
        if (id <= 0)
            return BadRequest("Invalid person ID");
        if (person == null)
            return BadRequest("Person data is required");

        if (!ModelState.IsValid)
        {
            var errors = ModelState
                .Where(x => x.Value.Errors.Count > 0)
                .ToDictionary(
                    kvp => kvp.Key,
                    kvp => kvp.Value.Errors.Select(e => e.ErrorMessage).ToArray()
                );

            return BadRequest(new
            {
                error = "Validation failed",
                details = "One or more validation errors occurred",
                validationErrors = errors
            });
        }

        try
        {
            // Check if person exists
            var existing = await _personRepo.ReadByIdAsync(id);
            if (existing == null)
                return NotFound($"Person with ID {id} not found");

            person.Id = id;
            var updated = await _personRepo.UpdateAsync(person);
            return Ok(updated);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to update person", details = ex.Message });
        }
    }

    [HttpDelete("{id}")]
    public async Task<IActionResult> DeletePerson(int id)
    {
        if (id <= 0)
            return BadRequest("Invalid person ID");

        try
        {
            // Check if person exists
            var existing = await _personRepo.ReadByIdAsync(id);
            if (existing == null)
                return NotFound($"Person with ID {id} not found");

            await _personRepo.DeleteByIdAsync(id);
            return NoContent();
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to delete person", details = ex.Message });
        }
    }
}