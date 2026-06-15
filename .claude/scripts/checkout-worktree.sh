#!/bin/bash

# Script to checkout/switch to an existing worktree
# Usage: Run from main repo to see list and select which worktree to switch to

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

info "Available worktrees:"
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

# Check if there are any worktrees
if [ ${#WORKTREE_OPTIONS[@]} -eq 0 ]; then
    warning "No worktrees found"
    info "Create a worktree first using: .claude/scripts/create-worktree.sh \"description\""
    exit 0
fi

# Display worktrees
highlight "Select a worktree to switch to:"
echo ""
for i in "${!WORKTREE_OPTIONS[@]}"; do
    echo "  $((i+1)). ${WORKTREE_OPTIONS[$i]}"
done
echo ""

# Prompt for selection
while true; do
    read -p "Select worktree (1-${#WORKTREE_OPTIONS[@]}) or 'q' to quit: " selection

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
success "Switching to worktree: $WORKTREE_BRANCH"
info "Path: $WORKTREE_PATH"
echo ""

# Navigate to worktree and start Claude
cd "$WORKTREE_PATH" && exec claude
