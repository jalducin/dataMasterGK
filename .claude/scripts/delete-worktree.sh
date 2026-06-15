#!/bin/bash

# Script to delete/remove a worktree and its branch
# Usage: Run from main repo to see list and select which worktree to delete

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored messages
error() {
    echo -e "${RED}Error: $1${NC}" >&2
}

success() {
    echo -e "${GREEN}✓ $1${NC}"
}

warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

info() {
    echo "$1"
}

highlight() {
    echo -e "${BLUE}$1${NC}"
}

# Check if we're in a git repository
if ! git rev-parse --git-dir > /dev/null 2>&1; then
    error "Not in a git repository"
    exit 1
fi

# Get main worktree path
MAIN_WORKTREE_PATH=$(git worktree list | head -1 | awk '{print $1}')

# Make sure we're in the main worktree
cd "$MAIN_WORKTREE_PATH"

info "Available worktrees to delete:"
echo ""

# Parse worktrees and filter out main
WORKTREE_OPTIONS=()
WORKTREE_PATHS=()
WORKTREE_BRANCHES=()

# Read git worktree list line by line
while IFS= read -r line; do
    # Skip empty lines
    [ -z "$line" ] && continue

    # Extract path and branch from "git worktree list" output
    # Format: /path/to/worktree commit [branch]
    path=$(echo "$line" | awk '{print $1}')
    branch=$(echo "$line" | grep -o '\[.*\]' | tr -d '[]')

    # Skip main worktree, entries without branches, and non-existent paths (prunable)
    if [ "$path" != "$MAIN_WORKTREE_PATH" ] && [ -n "$branch" ] && [ -d "$path" ]; then
        WORKTREE_PATHS+=("$path")
        WORKTREE_BRANCHES+=("$branch")

        # Get commit info and check for uncommitted changes
        commit_info=$(cd "$path" && git log -1 --oneline 2>/dev/null || echo "unknown")

        # Check for uncommitted changes
        uncommitted=""
        if ! (cd "$path" && git diff-index --quiet HEAD -- 2>/dev/null); then
            uncommitted=" ${YELLOW}[uncommitted changes]${NC}"
        fi

        WORKTREE_OPTIONS+=("$branch - $commit_info$uncommitted")
    fi
done < <(git worktree list)

# Check if there are any worktrees to delete
if [ ${#WORKTREE_OPTIONS[@]} -eq 0 ]; then
    warning "No worktrees found to delete"
    info "All worktrees have already been removed or pruned"
    exit 0
fi

# Display worktrees
highlight "Select a worktree to delete:"
echo ""
for i in "${!WORKTREE_OPTIONS[@]}"; do
    echo -e "  $((i+1)). ${WORKTREE_OPTIONS[$i]}"
done
echo ""

# Prompt for selection
while true; do
    read -p "Select worktree to delete (1-${#WORKTREE_OPTIONS[@]}) or 'q' to quit: " selection

    if [[ "$selection" == "q" ]] || [[ "$selection" == "Q" ]]; then
        info "Cancelled"
        exit 0
    fi

    if [[ "$selection" =~ ^[0-9]+$ ]] && [ "$selection" -ge 1 ] && [ "$selection" -le "${#WORKTREE_OPTIONS[@]}" ]; then
        idx=$((selection-1))
        WORKTREE_PATH="${WORKTREE_PATHS[$idx]}"
        WORKTREE_BRANCH="${WORKTREE_BRANCHES[$idx]}"
        break
    else
        error "Invalid selection. Please enter a number between 1 and ${#WORKTREE_OPTIONS[@]}"
    fi
done

echo ""
info "Deleting worktree: $WORKTREE_BRANCH"
echo ""

# Remove the worktree (force to handle uncommitted changes)
info "Removing worktree at: $WORKTREE_PATH"
if git worktree remove --force "$WORKTREE_PATH" 2>/dev/null; then
    success "Worktree removed"
else
    error "Failed to remove worktree"
    exit 1
fi

# Delete the branch
info "Deleting branch: $WORKTREE_BRANCH"
if git branch -D "$WORKTREE_BRANCH" 2>/dev/null; then
    success "Branch deleted"
else
    warning "Could not delete branch automatically"
    info "You may need to delete it manually: git branch -D $WORKTREE_BRANCH"
fi

echo ""
success "Deletion complete!"
info "Worktree '$WORKTREE_BRANCH' has been removed"
echo ""

# Show remaining worktrees
remaining=$(git worktree list | tail -n +2 | wc -l | tr -d ' ')
if [ "$remaining" -gt 0 ]; then
    info "Remaining worktrees: $remaining"
else
    info "No worktrees remaining (only main repository)"
fi
