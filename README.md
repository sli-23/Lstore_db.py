# ECS 165A: L-Store

## Team members & Contributors
- **Zihao Huang** (Developer, Tester, System Architect)
- **Qian Yao** (Developer, Tester, System Architect)
- **Justin Wallasch** (Developer, Tester)
- **Shuying Li** (Developer, Team Coordinator)

## Instructors
**Professor**: [Mohammad Sadoghi](https://expolab.org/)

**Teaching Fellow**: Sajjad Rahnama

**Teaching Fellow**: Shesha Vishnu Prasad

# Contents 
- [ECS 165A: L-Store](#ecs-165a-l-store)
  - [Team members & Contributors](#team-members--contributors)
  - [Instructors](#instructors)
- [Contents](#contents)
  - [L-Store Fundamentals](#l-store-fundamentals)
    - [Introduction](#introduction)
    - [Date Model](#date-model)
    - [Bufferpool Management](#bufferpool-management)
    - [Query_Interface](#query_interface)
  - [Milestones](#milestones)
    - [Milestone 1](#milestone-1)
      - [Structure Overview](#structure-overview)
      - [Presentation](#presentation)
    - [Milestone 2](#milestone-2)
      - [Restructure Details](#restructure-details)
      - [Presentation](#presentation-1)
    - [Milestone 3](#milestone-3)
      - [Presentation](#presentation-2)
  - [Sources](#sources)

## L-Store Fundamentals
### Introduction
Lineage-based Data Store (**L-Store**) is a solution that combines the real-time processing of transactional and analytical workloads within a single unified engine by introducing a novel update-friendly lineage-based storage architecture.

### Date Model
* Data storage in L-Store is columnar. The key idea is to separate the original version of a record inserted into the database (**base record**) and the subsequent updates to it (**tail records**)
* 

---
### Bufferpool Management

---
### Query_Interface

---
## Milestones
### Milestone 1
**Objectives**: Single-threaded, In-memory L-Store
* we focused on a simplified in-memory (volatile) implementation that provides basic relational data storage and querying capabilities.

**Project Description**: [Milestone 1](https://expolab.org/ecs165a-winter2022/milestones/Milestone1.pdf)

#### Structure Overview


#### Presentation
[Milestone 1: Single-threaded, In-memory L-Store](https://docs.google.com/presentation/d/1xfVZv24Y0t1clj_643fu7V2vQSKsAg_whh3AqZNLI50/edit?usp=sharing)

---
### Milestone 2
**Objectives**: Single-threaded, In-memory & Durable L-Store
* we focused on data durability by persisting data on a disk (non-volatile) and merging the base and tail data.

#### Restructure Details

#### Presentation

---
### Milestone 3
**Objectives**: Multi-threaded, In-memory & Durable L-Store
* We focused on concurrency and multi-threaded transaction processing.

#### Presentation

---
## Sources
1. [ECS 165A: Database Systems - Winter 2022](https://expolab.org/ecs165a-winter2022.html)
2. [Reference: Code Skeleton](https://github.com/msadoghi/165a-winter-2022)
3. [L-Store Paper](https://www.researchgate.net/publication/324150481_L-Store_A_Real-time_OLTP_and_OLAP_System)
4. [L-Store Sliders](https://expolab.org/papers/l-store-slides.pdf)