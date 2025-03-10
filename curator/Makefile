# Compiler and flags
CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra
LDFLAGS = -lrdkafka -lrdkafka++ -lmosquitto -lpthread

# Directories
SRC_DIR = src
WARNINGS_FILE = warnings.txt

# Find all .cpp files in the src folder
SRCS = $(wildcard $(SRC_DIR)/*.cpp)

# Generate corresponding binary paths for the current directory
BINARIES = $(patsubst $(SRC_DIR)/%.cpp,%,$(SRCS))

# Default target (Always cleans before compiling)
all: clean $(BINARIES)

# Rule to compile each binary in the current directory
%: $(SRC_DIR)/%.cpp
	@echo "Compiling $< -> $@"
	@bash -c '$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS) 2> >(grep -v "warning" >> $(WARNINGS_FILE))'

# Clean all binaries and warnings log
clean:
	@echo "Cleaning current directory..."
	@rm -f $(BINARIES) $(WARNINGS_FILE)

# Phony targets (Prevent conflicts with file names)
.PHONY: all clean
