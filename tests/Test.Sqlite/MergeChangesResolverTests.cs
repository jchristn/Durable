namespace Test.Sqlite
{
    using System;
    using System.Collections.Generic;
    using Durable;
    using Durable.ConcurrencyConflictResolvers;
    using Xunit;
    
    public class MergeChangesResolverTests
    {
        private readonly MergeChangesResolver<TestEntity> Resolver;
        
        public MergeChangesResolverTests()
        {
            Resolver = new MergeChangesResolver<TestEntity>("Id");
        }
        
        [Fact]
        public void ResolveConflict_NoChanges_KeepsOriginalValues()
        {
            TestEntity original = new TestEntity { Id = 1, Name = "Original", Value = 10, IsActive = true };
            TestEntity current = new TestEntity { Id = 1, Name = "Original", Value = 10, IsActive = true };
            TestEntity incoming = new TestEntity { Id = 1, Name = "Original", Value = 10, IsActive = true };
            
            TestEntity result = Resolver.ResolveConflict(current, incoming, original, ConflictResolutionStrategy.MergeChanges);
            
            Assert.Equal("Original", result.Name);
            Assert.Equal(10, result.Value);
            Assert.True(result.IsActive);
        }
        
        [Fact]
        public void ResolveConflict_OnlyCurrentChanged_UsesCurrentValues()
        {
            TestEntity original = new TestEntity { Id = 1, Name = "Original", Value = 10, IsActive = true };
            TestEntity current = new TestEntity { Id = 1, Name = "Current Update", Value = 20, IsActive = false };
            TestEntity incoming = new TestEntity { Id = 1, Name = "Original", Value = 10, IsActive = true };
            
            TestEntity result = Resolver.ResolveConflict(current, incoming, original, ConflictResolutionStrategy.MergeChanges);
            
            Assert.Equal("Current Update", result.Name);
            Assert.Equal(20, result.Value);
            Assert.False(result.IsActive);
        }
        
        [Fact]
        public void ResolveConflict_OnlyIncomingChanged_UsesIncomingValues()
        {
            TestEntity original = new TestEntity { Id = 1, Name = "Original", Value = 10, IsActive = true };
            TestEntity current = new TestEntity { Id = 1, Name = "Original", Value = 10, IsActive = true };
            TestEntity incoming = new TestEntity { Id = 1, Name = "Incoming Update", Value = 30, IsActive = false };
            
            TestEntity result = Resolver.ResolveConflict(current, incoming, original, ConflictResolutionStrategy.MergeChanges);
            
            Assert.Equal("Incoming Update", result.Name);
            Assert.Equal(30, result.Value);
            Assert.False(result.IsActive);
        }
        
        [Fact]
        public void ResolveConflict_BothChanged_DifferentProperties_MergesCorrectly()
        {
            TestEntity original = new TestEntity { Id = 1, Name = "Original", Value = 10, IsActive = true };
            TestEntity current = new TestEntity { Id = 1, Name = "Current Update", Value = 10, IsActive = true };
            TestEntity incoming = new TestEntity { Id = 1, Name = "Original", Value = 30, IsActive = false };
            
            TestEntity result = Resolver.ResolveConflict(current, incoming, original, ConflictResolutionStrategy.MergeChanges);
            
            Assert.Equal("Current Update", result.Name); // Only current changed
            Assert.Equal(30, result.Value); // Only incoming changed
            Assert.False(result.IsActive); // Only incoming changed
        }
        
        [Fact] 
        public void ResolveConflict_BothChangedSameProperty_UsesIncoming()
        {
            TestEntity original = new TestEntity { Id = 1, Name = "Original", Value = 10, IsActive = true };
            TestEntity current = new TestEntity { Id = 1, Name = "Current Update", Value = 10, IsActive = true };
            TestEntity incoming = new TestEntity { Id = 1, Name = "Incoming Update", Value = 10, IsActive = true };
            
            TestEntity result = Resolver.ResolveConflict(current, incoming, original, ConflictResolutionStrategy.MergeChanges);
            
            // When both change the same property, incoming wins
            Assert.Equal("Incoming Update", result.Name);
        }
        
        [Fact]
        public void ResolveConflict_WithNullValues_HandlesCorrectly()
        {
            TestEntity original = new TestEntity { Id = 1, Name = "Original", Value = 10, Description = null };
            TestEntity current = new TestEntity { Id = 1, Name = "Original", Value = 10, Description = "Added by current" };
            TestEntity incoming = new TestEntity { Id = 1, Name = "Original", Value = 10, Description = null };
            
            TestEntity result = Resolver.ResolveConflict(current, incoming, original, ConflictResolutionStrategy.MergeChanges);
            
            Assert.Equal("Added by current", result.Description); // Only current changed from null
        }
        
        [Fact]
        public void ResolveConflict_NullToNonNull_HandlesCorrectly()
        {
            TestEntity original = new TestEntity { Id = 1, Name = "Original", Value = 10, Description = null };
            TestEntity current = new TestEntity { Id = 1, Name = "Original", Value = 10, Description = null };
            TestEntity incoming = new TestEntity { Id = 1, Name = "Original", Value = 10, Description = "Added by incoming" };
            
            TestEntity result = Resolver.ResolveConflict(current, incoming, original, ConflictResolutionStrategy.MergeChanges);
            
            Assert.Equal("Added by incoming", result.Description); // Only incoming changed from null
        }
        
        [Fact]
        public void ResolveConflict_NonNullToNull_HandlesCorrectly()
        {
            TestEntity original = new TestEntity { Id = 1, Name = "Original", Value = 10, Description = "Has value" };
            TestEntity current = new TestEntity { Id = 1, Name = "Original", Value = 10, Description = null };
            TestEntity incoming = new TestEntity { Id = 1, Name = "Original", Value = 10, Description = "Has value" };
            
            TestEntity result = Resolver.ResolveConflict(current, incoming, original, ConflictResolutionStrategy.MergeChanges);
            
            Assert.Null(result.Description); // Only current changed to null
        }
        
        [Fact]
        public void ResolveConflict_IgnoredProperties_UsesCurrentValue()
        {
            TestEntity original = new TestEntity { Id = 1, Name = "Original", Value = 10, IsActive = true };
            TestEntity current = new TestEntity { Id = 2, Name = "Original", Value = 10, IsActive = true }; // Id changed in current
            TestEntity incoming = new TestEntity { Id = 3, Name = "Original", Value = 10, IsActive = true }; // Id changed in incoming
            
            TestEntity result = Resolver.ResolveConflict(current, incoming, original, ConflictResolutionStrategy.MergeChanges);
            
            // Id is ignored, so should use current value
            Assert.Equal(2, result.Id);
        }
        
        [Fact]
        public void ResolveConflict_NullEntities_ThrowsException()
        {
            TestEntity entity = new TestEntity { Id = 1, Name = "Test", Value = 10, IsActive = true };
            
            Assert.Throws<ArgumentNullException>(() => 
                Resolver.ResolveConflict(null, entity, entity, ConflictResolutionStrategy.MergeChanges));
                
            Assert.Throws<ArgumentNullException>(() => 
                Resolver.ResolveConflict(entity, null, entity, ConflictResolutionStrategy.MergeChanges));
                
            Assert.Throws<ArgumentNullException>(() => 
                Resolver.ResolveConflict(entity, entity, null, ConflictResolutionStrategy.MergeChanges));
        }
    }
}