#!/bin/bash

# Script to merge a worktree branch into main and clean up
# Usage:
#   - Run from main repo: Shows interactive list of worktrees to merge
#   - Run from worktree: Automatically merges current worktree

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

# Get current directory info
CURRENT_DIR=$(pwd)
MAIN_WORKTREE_PATH=$(git worktree list | head -1 | awk '{print $1}')

# Determine if we're in main repo or a worktree
IS_MAIN_REPO=false
if [ ! -f "$CURRENT_DIR/.git" ]; then
    IS_MAIN_REPO=true
fi

WORKTREE_PATH=""
WORKTREE_BRANCH=""

# If running from main repo, show interactive selection
if [ "$IS_MAIN_REPO" = true ]; then
    info "Running from main repository"
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

            # Get commit info
            commit_info=$(cd "$path" && git log -1 --oneline 2>/dev/null || echo "unknown")
            WORKTREE_OPTIONS+=("$branch - $commit_info")
        fi
    done < <(git worktree list)

    # Check if there are any worktrees to merge
    if [ ${#WORKTREE_OPTIONS[@]} -eq 0 ]; then
        warning "No worktrees found to merge"
        info "Create a worktree first using: .claude/scripts/create-worktree.sh \"description\""
        exit 0
    fi

    # Display worktrees
    highlight "Available worktrees to merge:"
    echo ""
    for i in "${!WORKTREE_OPTIONS[@]}"; do
        echo "  $((i+1)). ${WORKTREE_OPTIONS[$i]}"
    done
    echo ""

    # Prompt for selection
    while true; do
        read -p "Select worktree to merge (1-${#WORKTREE_OPTIONS[@]}) or 'q' to quit: " selection

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
    info "Selected: $WORKTREE_BRANCH"
    echo ""

    # Check for uncommitted changes in selected worktree
    cd "$WORKTREE_PATH"
    if ! git diff-index --quiet HEAD -- 2>/dev/null; then
        error "The selected worktree has uncommitted changes"
        info "Please commit or stash your changes before merging"
        cd "$WORKTREE_PATH" && git status --short
        exit 1
    fi

    # Go back to main repo
    cd "$MAIN_WORKTREE_PATH"

else
    # Running from within a worktree
    CURRENT_BRANCH=$(git branch --show-current)
    WORKTREE_PATH="$CURRENT_DIR"
    WORKTREE_BRANCH="$CURRENT_BRANCH"

    info "Detected worktree: $CURRENT_DIR"
    info "Current branch: $CURRENT_BRANCH"
    echo ""

    # Check for uncommitted changes
    if ! git diff-index --quiet HEAD --; then
        error "You have uncommitted changes in this worktree"
        info "Please commit or stash your changes before merging"
        git status --short
        exit 1
    fi
fi

# Check if there are unpushed commits (informational only)
if (cd "$WORKTREE_PATH" && git log --branches --not --remotes 2>/dev/null | grep -q .); then
    warning "This branch has commits that haven't been pushed to remote"
    info "Continuing with local merge..."
    echo ""
fi

# Navigate to main worktree
info "Switching to main worktree..."
cd "$MAIN_WORKTREE_PATH"

# Verify we're on main branch or switch to it
MAIN_BRANCH="main"
if ! git show-ref --verify --quiet "refs/heads/$MAIN_BRANCH"; then
    # Try 'master' as fallback
    MAIN_BRANCH="master"
    if ! git show-ref --verify --quiet "refs/heads/$MAIN_BRANCH"; then
        error "Could not find main or master branch"
        exit 1
    fi
fi

if [ "$(git branch --show-current)" != "$MAIN_BRANCH" ]; then
    info "Checking out $MAIN_BRANCH branch..."
    git checkout "$MAIN_BRANCH"
fi

success "On $MAIN_BRANCH branch in main worktree"
echo ""

# Attempt merge
info "Merging branch '$WORKTREE_BRANCH' into $MAIN_BRANCH..."
if ! GIT_EDITOR=true GIT_PAGER=cat git merge "$WORKTREE_BRANCH" --no-edit -m "Merge branch '$WORKTREE_BRANCH'"; then
    error "Merge conflict detected!"
    info ""
    info "The merge has been aborted and your worktree has been preserved."
    info "To resolve:"
    info "  1. Stay in main worktree: cd $MAIN_WORKTREE_PATH"
    info "  2. Run: git merge $WORKTREE_BRANCH"
    info "  3. Resolve conflicts manually"
    info "  4. Commit the merge"
    info "  5. Run this script again to clean up the worktree"
    info ""
    info "Worktree location: $WORKTREE_PATH"

    # Abort the merge
    git merge --abort
    exit 1
fi

success "Merge successful!"
echo ""

# Cleanup: Remove worktree and delete branch
info "Cleaning up..."

# Navigate out of the worktree directory to avoid issues
cd "$MAIN_WORKTREE_PATH"

# Remove the worktree
info "Removing worktree at: $WORKTREE_PATH"
if git worktree remove "$WORKTREE_PATH" 2>/dev/null; then
    success "Worktree removed"
else
    # Try with --force if normal remove fails (handles untracked files)
    warning "Worktree has untracked or modified files, forcing removal..."
    if git worktree remove --force "$WORKTREE_PATH"; then
        success "Worktree removed (forced)"
    else
        error "Could not remove worktree automatically"
        info "You may need to remove it manually: git worktree remove --force $WORKTREE_PATH"
        exit 1
    fi
fi

# Delete the branch
info "Deleting branch: $WORKTREE_BRANCH"
if git branch -d "$WORKTREE_BRANCH" 2>/dev/null; then
    success "Branch deleted"
elif git branch -D "$WORKTREE_BRANCH" 2>/dev/null; then
    success "Branch force-deleted (had unmerged commits)"
else
    warning "Could not delete branch automatically"
    info "You may need to delete it manually: git branch -d $WORKTREE_BRANCH"
fi

echo ""
success "Merge and cleanup complete!"
info "Branch '$WORKTREE_BRANCH' has been merged into $MAIN_BRANCH"
info "You are now in: $MAIN_WORKTREE_PATH"
echo ""

# Show recent commits
info "Recent commits on $MAIN_BRANCH:"
git --no-pager log --oneline -5
