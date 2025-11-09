#!/bin/bash

# run_all_demos.sh - Script untuk menjalankan semua demo

echo "========================================"
echo "DISTRIBUTED SYNC SYSTEM - FULL DEMO"
echo "========================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to check if containers are running
check_containers() {
    echo -e "\n${YELLOW}[CHECK]${NC} Checking Docker containers..."
    docker-compose ps
    
    # Wait for containers to be healthy
    echo -e "\n${YELLOW}[WAIT]${NC} Waiting for services to be ready (15 seconds)..."
    sleep 15
}

# Function to run demo with header
run_demo() {
    local demo_name=$1
    local demo_script=$2
    
    echo -e "\n\n"
    echo "========================================"
    echo "  $demo_name"
    echo "========================================"
    
    python "$demo_script"
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ $demo_name completed successfully!${NC}"
    else
        echo -e "${RED}✗ $demo_name failed!${NC}"
        return 1
    fi
    
    echo -e "\n${YELLOW}[PAUSE]${NC} Press Enter to continue to next demo..."
    read
}

# Main execution
main() {
    # 1. Check if in correct directory
    if [ ! -f "docker/docker-compose.yml" ]; then
        echo -e "${RED}Error: Please run this script from the project root directory!${NC}"
        exit 1
    fi
    
    # 2. Build and start containers
    echo -e "\n${YELLOW}[SETUP]${NC} Building and starting containers..."
    cd docker
    docker-compose down -v  # Clean start
    docker-compose build
    docker-compose up -d
    cd ..
    
    check_containers
    
    # 3. Run Distributed Lock Demo
    run_demo "DISTRIBUTED LOCK DEMO" "src/demo/lock_demo.py"
    
    # 4. Run Queue Demo
    run_demo "DISTRIBUTED QUEUE DEMO" "src/demo/queue_demo.py"
    
    # 5. Run Cache Demo
    run_demo "DISTRIBUTED CACHE DEMO" "src/demo/cache_demo.py"
    
    # 6. Final status check
    echo -e "\n\n"
    echo "========================================"
    echo "  FINAL STATUS CHECK"
    echo "========================================"
    
    echo -e "\n${YELLOW}[1]${NC} Docker Containers:"
    docker-compose -f docker/docker-compose.yml ps
    
    echo -e "\n${YELLOW}[2]${NC} Node Status:"
    curl -s http://localhost:6001/status | python -m json.tool 2>/dev/null || echo "Node 1: Error"
    curl -s http://localhost:6002/status | python -m json.tool 2>/dev/null || echo "Node 2: Error"
    curl -s http://localhost:6003/status | python -m json.tool 2>/dev/null || echo "Node 3: Error"
    
    # 7. Cleanup option
    echo -e "\n\n${YELLOW}Would you like to stop and remove all containers? (y/n):${NC} "
    read -r response
    if [[ "$response" =~ ^[Yy]$ ]]; then
        echo -e "\n${YELLOW}[CLEANUP]${NC} Stopping containers..."
        cd docker
        docker-compose down -v
        cd ..
        echo -e "${GREEN}✓ Cleanup completed!${NC}"
    fi
    
    echo -e "\n\n"
    echo "========================================"
    echo "  ALL DEMOS COMPLETED!"
    echo "========================================"
    echo -e "${GREEN}Thank you for watching!${NC}"
}

# Run main function
main