using System;
using System.Collections.Generic;
using System.Reflection;
using Durable;
using Xunit;

namespace Test.Sqlite
{
    public class SimpleChangeTrackerTests
    {
        private readonly SimpleChangeTracker<TestEntityForTracking> _changeTracker;
        private readonly Dictionary<string, PropertyInfo> _columnMappings;

        public SimpleChangeTrackerTests()
        {
            _columnMappings = new Dictionary<string, PropertyInfo>
            {
                ["id"] = typeof(TestEntityForTracking).GetProperty(nameof(TestEntityForTracking.Id))!,
                ["name"] = typeof(TestEntityForTracking).GetProperty(nameof(TestEntityForTracking.Name))!,
                ["value"] = typeof(TestEntityForTracking).GetProperty(nameof(TestEntityForTracking.Value))!,
                ["is_active"] = typeof(TestEntityForTracking).GetProperty(nameof(TestEntityForTracking.IsActive))!,
                ["description"] = typeof(TestEntityForTracking).GetProperty(nameof(TestEntityForTracking.Description))!
            };
            _changeTracker = new SimpleChangeTracker<TestEntityForTracking>(_columnMappings);
        }

        [Fact]
        public void Constructor_NullColumnMappings_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() => new SimpleChangeTracker<TestEntityForTracking>(null));
        }

        [Fact]
        public void TrackEntity_NewEntity_TracksOriginalValues()
        {
            TestEntityForTracking entity = new TestEntityForTracking
            {
                Id = 1,
                Name = "Test",
                Value = 100,
                IsActive = true,
                Description = "Original description"
            };

            _changeTracker.TrackEntity(entity);

            TestEntityForTracking original = _changeTracker.GetOriginalValues(entity);
            Assert.NotNull(original);
            Assert.Equal(1, original.Id);
            Assert.Equal("Test", original.Name);
            Assert.Equal(100, original.Value);
            Assert.True(original.IsActive);
            Assert.Equal("Original description", original.Description);
        }

        [Fact]
        public void TrackEntity_NullEntity_DoesNotThrow()
        {
            _changeTracker.TrackEntity(null); // Should not throw
        }

        [Fact]
        public void TrackEntity_UpdateExistingTracking_UpdatesOriginalValues()
        {
            TestEntityForTracking entity = new TestEntityForTracking
            {
                Id = 1,
                Name = "Original",
                Value = 100
            };

            _changeTracker.TrackEntity(entity);

            // Update entity
            entity.Name = "Modified";
            entity.Value = 200;

            // Track again with updated values
            _changeTracker.TrackEntity(entity);

            // Original should reflect the new tracking state
            TestEntityForTracking original = _changeTracker.GetOriginalValues(entity);
            Assert.Equal("Modified", original.Name);
            Assert.Equal(200, original.Value);
        }

        [Fact]
        public void GetOriginalValues_NullEntity_ReturnsNull()
        {
            TestEntityForTracking result = _changeTracker.GetOriginalValues(null);
            Assert.Null(result);
        }

        [Fact]
        public void GetOriginalValues_UntrackedEntity_ReturnsNull()
        {
            TestEntityForTracking entity = new TestEntityForTracking { Id = 1, Name = "Test" };
            TestEntityForTracking result = _changeTracker.GetOriginalValues(entity);
            Assert.Null(result);
        }

        [Fact]
        public void HasChanges_NullEntity_ReturnsFalse()
        {
            bool result = _changeTracker.HasChanges(null);
            Assert.False(result);
        }

        [Fact]
        public void HasChanges_UntrackedEntity_ReturnsFalse()
        {
            TestEntityForTracking entity = new TestEntityForTracking { Id = 1, Name = "Test" };
            bool result = _changeTracker.HasChanges(entity);
            Assert.False(result);
        }

        [Fact]
        public void HasChanges_NoChanges_ReturnsFalse()
        {
            TestEntityForTracking entity = new TestEntityForTracking
            {
                Id = 1,
                Name = "Test",
                Value = 100,
                IsActive = true
            };

            _changeTracker.TrackEntity(entity);

            bool result = _changeTracker.HasChanges(entity);
            Assert.False(result);
        }

        [Fact]
        public void HasChanges_StringPropertyChanged_ReturnsTrue()
        {
            TestEntityForTracking entity = new TestEntityForTracking
            {
                Id = 1,
                Name = "Original",
                Value = 100
            };

            _changeTracker.TrackEntity(entity);

            entity.Name = "Modified";

            bool result = _changeTracker.HasChanges(entity);
            Assert.True(result);
        }

        [Fact]
        public void HasChanges_IntPropertyChanged_ReturnsTrue()
        {
            TestEntityForTracking entity = new TestEntityForTracking
            {
                Id = 1,
                Name = "Test",
                Value = 100
            };

            _changeTracker.TrackEntity(entity);

            entity.Value = 200;

            bool result = _changeTracker.HasChanges(entity);
            Assert.True(result);
        }

        [Fact]
        public void HasChanges_BoolPropertyChanged_ReturnsTrue()
        {
            TestEntityForTracking entity = new TestEntityForTracking
            {
                Id = 1,
                Name = "Test",
                IsActive = true
            };

            _changeTracker.TrackEntity(entity);

            entity.IsActive = false;

            bool result = _changeTracker.HasChanges(entity);
            Assert.True(result);
        }

        [Fact]
        public void HasChanges_NullToValue_ReturnsTrue()
        {
            TestEntityForTracking entity = new TestEntityForTracking
            {
                Id = 1,
                Name = "Test",
                Description = null
            };

            _changeTracker.TrackEntity(entity);

            entity.Description = "New description";

            bool result = _changeTracker.HasChanges(entity);
            Assert.True(result);
        }

        [Fact]
        public void HasChanges_ValueToNull_ReturnsTrue()
        {
            TestEntityForTracking entity = new TestEntityForTracking
            {
                Id = 1,
                Name = "Test",
                Description = "Original description"
            };

            _changeTracker.TrackEntity(entity);

            entity.Description = null;

            bool result = _changeTracker.HasChanges(entity);
            Assert.True(result);
        }

        [Fact]
        public void HasChanges_NullToNull_ReturnsFalse()
        {
            TestEntityForTracking entity = new TestEntityForTracking
            {
                Id = 1,
                Name = "Test",
                Description = null
            };

            _changeTracker.TrackEntity(entity);

            // Description remains null
            
            bool result = _changeTracker.HasChanges(entity);
            Assert.False(result);
        }

        [Fact]
        public void HasChanges_SameValueToSameValue_ReturnsFalse()
        {
            TestEntityForTracking entity = new TestEntityForTracking
            {
                Id = 1,
                Name = "Test",
                Description = "Same value"
            };

            _changeTracker.TrackEntity(entity);

            entity.Description = "Same value"; // Same value

            bool result = _changeTracker.HasChanges(entity);
            Assert.False(result);
        }

        [Fact]
        public void HasChanges_MultiplePropertiesChanged_ReturnsTrue()
        {
            TestEntityForTracking entity = new TestEntityForTracking
            {
                Id = 1,
                Name = "Original",
                Value = 100,
                IsActive = true
            };

            _changeTracker.TrackEntity(entity);

            entity.Name = "Modified";
            entity.Value = 200;
            entity.IsActive = false;

            bool result = _changeTracker.HasChanges(entity);
            Assert.True(result);
        }

        [Fact]
        public void StopTracking_TrackedEntity_RemovesFromTracking()
        {
            TestEntityForTracking entity = new TestEntityForTracking
            {
                Id = 1,
                Name = "Test"
            };

            _changeTracker.TrackEntity(entity);

            Assert.NotNull(_changeTracker.GetOriginalValues(entity));

            _changeTracker.StopTracking(entity);

            Assert.Null(_changeTracker.GetOriginalValues(entity));
        }

        [Fact]
        public void StopTracking_NullEntity_DoesNotThrow()
        {
            _changeTracker.StopTracking(null); // Should not throw
        }

        [Fact]
        public void StopTracking_UntrackedEntity_DoesNotThrow()
        {
            TestEntityForTracking entity = new TestEntityForTracking { Id = 1, Name = "Test" };
            _changeTracker.StopTracking(entity); // Should not throw
        }

        [Fact]
        public void Clear_RemovesAllTrackedEntities()
        {
            TestEntityForTracking entity1 = new TestEntityForTracking { Id = 1, Name = "Test1" };
            TestEntityForTracking entity2 = new TestEntityForTracking { Id = 2, Name = "Test2" };

            _changeTracker.TrackEntity(entity1);
            _changeTracker.TrackEntity(entity2);

            Assert.NotNull(_changeTracker.GetOriginalValues(entity1));
            Assert.NotNull(_changeTracker.GetOriginalValues(entity2));

            _changeTracker.Clear();

            Assert.Null(_changeTracker.GetOriginalValues(entity1));
            Assert.Null(_changeTracker.GetOriginalValues(entity2));
        }

        [Fact]
        public void HasChanges_ReadOnlyProperty_IgnoresProperty()
        {
            var mappings = new Dictionary<string, PropertyInfo>
            {
                ["id"] = typeof(TestEntityWithReadOnlyProperty).GetProperty(nameof(TestEntityWithReadOnlyProperty.Id))!,
                ["readonly_prop"] = typeof(TestEntityWithReadOnlyProperty).GetProperty(nameof(TestEntityWithReadOnlyProperty.ReadOnlyProperty))!
            };

            var tracker = new SimpleChangeTracker<TestEntityWithReadOnlyProperty>(mappings);

            TestEntityWithReadOnlyProperty entity = new TestEntityWithReadOnlyProperty { Id = 1 };
            tracker.TrackEntity(entity);

            // Since ReadOnlyProperty can't be written, it should be ignored during tracking
            bool result = tracker.HasChanges(entity);
            Assert.False(result);
        }

        [Fact]
        public void TrackEntity_ConcurrentAccess_HandlesCorrectly()
        {
            TestEntityForTracking entity = new TestEntityForTracking
            {
                Id = 1,
                Name = "Test"
            };

            // Simulate concurrent tracking of the same entity
            _changeTracker.TrackEntity(entity);
            _changeTracker.TrackEntity(entity); // Should update, not fail

            Assert.NotNull(_changeTracker.GetOriginalValues(entity));
        }
    }

    public class TestEntityForTracking
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Value { get; set; }
        public bool IsActive { get; set; }
        public string? Description { get; set; }
    }

    public class TestEntityWithReadOnlyProperty
    {
        public int Id { get; set; }
        public string ReadOnlyProperty { get; } = "Cannot be written";
    }
}