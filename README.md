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
      - [Insert](#insert)
      - [Select](#select)
      - [Update](#update)
      - [Sum](#sum)
      - [Delete](#delete)
  - [Milestones](#milestones)
    - [Milestone 1](#milestone-1)
      - [Reconstruction Overview](#reconstruction-overview)
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

---
### Bufferpool Management

---
### Query_Interface
#### Insert

#### Select

#### Update

#### Sum

#### Delete

---
## Milestones
### Milestone 1
**Objectives**: Single-threaded, In-memory L-Store
* we focused on a simplified in-memory (volatile) implementation that provides basic relational data storage and querying capabilities.

```python
"""
Global Setting:

* Page size: 4096 kb 
* Maximum records per page: 4096 / 8 = 512
* Maximum page per Multipage: 16 pages
* Maximum records per Multipage: 512 * 16 records
* MAXINT： maximum numbers in 8 bytes can be stored

Meta_data (Default Column):
0: Indirection (MAXINT; using the constant MAXINT to dectect if there is any updates in that records)
1: RID: numbers of records
2: Time: current time
3: Schema Encoding: if there is no updates, the column will be '1' (example, "00000", with update in column 3 will be "00100")

"""
```

```python
"""
Data Model in Milestone 1:

Page Directory {}
|_______________Base Pages[[Multipage 1], [Multipage 2]], Tail Pages[[Page 1], [Page 2]]

Table
|
Page()
|______Multipages (16 * [Page()]); multipage.pages = [Page 1, Page 2, Page 3, Page 4]
"""
```

```Python
"""
Query workflow:

Insert:
* 
"""
```

**Project Description**: [Milestone 1](https://expolab.org/ecs165a-winter2022/milestones/Milestone1.pdf)

#### Reconstruction Overview
* Applying Index in data structure



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