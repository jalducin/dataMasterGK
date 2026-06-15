#!/bin/bash

# Script to create a git worktree and start Claude in it
# Usage: ./create-worktree.sh "short description"

set -e

if [ -z "$1" ]; then
    echo "Error: Description is required"
    echo "Usage: $0 \"short description\""
    exit 1
fi

description="$1"

# Use Claude to generate branch name
echo "Generating branch name..."
branch_name=$(claude --print "Convert this to a git branch name: '$description'. Rules: lowercase, use hyphens instead of spaces, no slashes, no prefixes like feature/ or fix/. Return ONLY the branch name, nothing else.")

# Trim whitespace
branch_name=$(echo "$branch_name" | xargs)

echo "Branch name: $branch_name"

# Check if we're in a git repository
if ! git rev-parse --git-dir > /dev/null 2>&1; then
    echo "Error: Not in a git repository"
    exit 1
fi

# Get the project root folder name
PROJECT_ROOT=$(basename "$(git rev-parse --show-toplevel)")
WORKTREE_BASE="../${PROJECT_ROOT}-worktrees"
WORKTREE_PATH="$WORKTREE_BASE/$branch_name"

# Create the worktree base directory if it doesn't exist
mkdir -p "$WORKTREE_BASE"

# Check if the worktree path already exists
if [ -d "$WORKTREE_PATH" ]; then
    echo "Error: Worktree path '$WORKTREE_PATH' already exists"
    exit 1
fi

# Create worktree
if git show-ref --verify --quiet "refs/heads/$branch_name"; then
    # Check if branch is already used by another worktree
    if git worktree list | grep -q "$branch_name"; then
        echo "Error: Branch '$branch_name' is already used by another worktree"
        echo "Existing worktrees:"
        git worktree list
        exit 1
    fi
    echo "Branch '$branch_name' already exists. Creating worktree with existing branch..."
    git worktree add "$WORKTREE_PATH" "$branch_name"
else
    echo "Creating new branch '$branch_name' and worktree..."
    git worktree add -b "$branch_name" "$WORKTREE_PATH"
fi

echo ""
echo "✓ Worktree created at: $WORKTREE_PATH"
echo "✓ Starting Claude in worktree..."
echo ""

# CD into worktree and start claude
cd "$WORKTREE_PATH" && exec claude
