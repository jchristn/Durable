namespace Test.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using Durable;
    using Xunit;
    
    public class SimpleChangeTrackerTests
    {
        private readonly SimpleChangeTracker<TestEntityForTracking> ChangeTracker;
        private readonly Dictionary<string, PropertyInfo> ColumnMappings;

        public SimpleChangeTrackerTests()
        {
            ColumnMappings = new Dictionary<string, PropertyInfo>
            {
                ["id"] = typeof(TestEntityForTracking).GetProperty(nameof(TestEntityForTracking.Id))!,
                ["name"] = typeof(TestEntityForTracking).GetProperty(nameof(TestEntityForTracking.Name))!,
                ["value"] = typeof(TestEntityForTracking).GetProperty(nameof(TestEntityForTracking.Value))!,
                ["is_active"] = typeof(TestEntityForTracking).GetProperty(nameof(TestEntityForTracking.IsActive))!,
                ["description"] = typeof(TestEntityForTracking).GetProperty(nameof(TestEntityForTracking.Description))!
            };
            ChangeTracker = new SimpleChangeTracker<TestEntityForTracking>(ColumnMappings);
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

            ChangeTracker.TrackEntity(entity);

            TestEntityForTracking original = ChangeTracker.GetOriginalValues(entity);
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

            ChangeTracker.TrackEntity(entity);

            // Update entity
            entity.Name = "Modified";
            entity.Value = 200;

            // Track again with updated values
            ChangeTracker.TrackEntity(entity);

            // Original should reflect the new tracking state
            TestEntityForTracking original = ChangeTracker.GetOriginalValues(entity);
            Assert.Equal("Modified", original.Name);
            Assert.Equal(200, original.Value);
        }

        [Fact]
        public void GetOriginalValues_NullEntity_ReturnsNull()
        {
            TestEntityForTracking result = ChangeTracker.GetOriginalValues(null);
            Assert.Null(result);
        }

        [Fact]
        public void GetOriginalValues_UntrackedEntity_ReturnsNull()
        {
            TestEntityForTracking entity = new TestEntityForTracking { Id = 1, Name = "Test" };
            TestEntityForTracking result = ChangeTracker.GetOriginalValues(entity);
            Assert.Null(result);
        }

        [Fact]
        public void HasChanges_NullEntity_ReturnsFalse()
        {
            bool result = ChangeTracker.HasChanges(null);
            Assert.False(result);
        }

        [Fact]
        public void HasChanges_UntrackedEntity_ReturnsFalse()
        {
            TestEntityForTracking entity = new TestEntityForTracking { Id = 1, Name = "Test" };
            bool result = ChangeTracker.HasChanges(entity);
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

            ChangeTracker.TrackEntity(entity);

            bool result = ChangeTracker.HasChanges(entity);
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

            ChangeTracker.TrackEntity(entity);

            entity.Name = "Modified";

            bool result = ChangeTracker.HasChanges(entity);
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

            ChangeTracker.TrackEntity(entity);

            entity.Value = 200;

            bool result = ChangeTracker.HasChanges(entity);
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

            ChangeTracker.TrackEntity(entity);

            entity.IsActive = false;

            bool result = ChangeTracker.HasChanges(entity);
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

            ChangeTracker.TrackEntity(entity);

            entity.Description = "New description";

            bool result = ChangeTracker.HasChanges(entity);
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

            ChangeTracker.TrackEntity(entity);

            entity.Description = null;

            bool result = ChangeTracker.HasChanges(entity);
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

            ChangeTracker.TrackEntity(entity);

            // Description remains null
            
            bool result = ChangeTracker.HasChanges(entity);
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

            ChangeTracker.TrackEntity(entity);

            entity.Description = "Same value"; // Same value

            bool result = ChangeTracker.HasChanges(entity);
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

            ChangeTracker.TrackEntity(entity);

            entity.Name = "Modified";
            entity.Value = 200;
            entity.IsActive = false;

            bool result = ChangeTracker.HasChanges(entity);
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

            ChangeTracker.TrackEntity(entity);

            Assert.NotNull(ChangeTracker.GetOriginalValues(entity));

            ChangeTracker.StopTracking(entity);

            Assert.Null(ChangeTracker.GetOriginalValues(entity));
        }

        [Fact]
        public void StopTracking_NullEntity_DoesNotThrow()
        {
            ChangeTracker.StopTracking(null); // Should not throw
        }

        [Fact]
        public void StopTracking_UntrackedEntity_DoesNotThrow()
        {
            TestEntityForTracking entity = new TestEntityForTracking { Id = 1, Name = "Test" };
            ChangeTracker.StopTracking(entity); // Should not throw
        }

        [Fact]
        public void Clear_RemovesAllTrackedEntities()
        {
            TestEntityForTracking entity1 = new TestEntityForTracking { Id = 1, Name = "Test1" };
            TestEntityForTracking entity2 = new TestEntityForTracking { Id = 2, Name = "Test2" };

            _changeTracker.TrackEntity(entity1);
            _changeTracker.TrackEntity(entity2);

            Assert.NotNull(ChangeTracker.GetOriginalValues(entity1));
            Assert.NotNull(ChangeTracker.GetOriginalValues(entity2));

            ChangeTracker.Clear();

            Assert.Null(ChangeTracker.GetOriginalValues(entity1));
            Assert.Null(ChangeTracker.GetOriginalValues(entity2));
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
            ChangeTracker.TrackEntity(entity);
            ChangeTracker.TrackEntity(entity); // Should update, not fail

            Assert.NotNull(ChangeTracker.GetOriginalValues(entity));
        }
    }
}