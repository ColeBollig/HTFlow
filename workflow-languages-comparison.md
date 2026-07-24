# Workflow Languages: Nextflow, WDL, CWL, and Snakemake

A comparison of four workflow languages commonly used in scientific computing and bioinformatics: how they execute, what they share, where they diverge, and what control-flow constructs (conditionals, loops, early exits) each one supports.

## Table of Contents

- [Overview](#overview)
  - [Nextflow](#nextflow)
  - [WDL (Workflow Description Language)](#wdl-workflow-description-language)
  - [CWL (Common Workflow Language)](#cwl-common-workflow-language)
  - [Snakemake](#snakemake)
- [Similarities](#similarities)
- [Differences](#differences)
- [Assumptions Baked Into Each Language](#assumptions-baked-into-each-language)
- [Control Flow & Language Constructs](#control-flow--language-constructs)
  - [Nextflow](#nextflow-1)
  - [WDL](#wdl)
  - [CWL](#cwl)
  - [Snakemake](#snakemake-1)
  - [Summary Table: Control Flow Constructs](#summary-table-control-flow-constructs)
  - [Takeaway](#takeaway)
- [Sources](#sources)

## Overview

These four languages all solve the same core problem — describing a pipeline of computational steps (typically command-line tools) with data dependencies between them, and letting an engine figure out execution order and parallelism — but they come from different communities and make different tradeoffs between flexibility, portability, and simplicity.

### Nextflow
A Groovy-based DSL (current version: DSL2) built on a **dataflow programming model**. Pipelines are composed of `process` blocks (units of work, each wrapping a script in any language) that communicate exclusively through asynchronous **channels**. There's no central scheduler computing a DAG upfront — a process fires automatically whenever data becomes available on all of its declared input channels, so parallelism emerges implicitly from the flow of data rather than from an explicit graph. DSL2 added modularity (reusable `modules`, `workflow` blocks that compose sub-workflows). Execution is handled by the Nextflow runtime (JVM-based), which supports pluggable executors (local, Slurm, PBS, AWS Batch, Kubernetes, etc.) and containerization (Docker/Singularity/Conda) per-process. Backed commercially by Seqera (nf-core is the associated community pipeline registry).

### WDL (Workflow Description Language)
A declarative, JSON/YAML-adjacent language governed by the OpenWDL project (not a single reference implementation). A workflow declares `task`s (a command template, inputs, outputs, runtime requirements like Docker image) and a `workflow` block that calls tasks and wires their inputs/outputs together, including `scatter` blocks for parallel fan-out. WDL itself doesn't execute anything — it's purely a specification; the actual work is done by separate engines: **Cromwell** (Broad Institute, production-grade, backs the Terra platform), **miniWDL** (CZI, lightweight, popular for local dev), **Toil**, **Sprocket**, and **Arvados**. The engine parses the WDL, statically resolves the task dependency graph, then dispatches tasks to a backend (local, HPC scheduler, cloud).

### CWL (Common Workflow Language)
A YAML/JSON-based declarative specification (current spec: v1.2) designed explicitly for portability across execution platforms. Everything is JSON-schema-validated data: `CommandLineTool` documents describe a single tool invocation (inputs, arguments, outputs, requirements), and `Workflow` documents wire tools together via explicit `steps` with typed input/output ports. Because it's built on JSON-LD, CWL documents are themselves data that can be introspected, generated, and validated by tooling — it's less "programming language," more "workflow interchange format." Execution engines include `cwltool` (the reference implementation), Toil, Arvados, Cromwell (partial CWL support), and StreamFlow.

### Snakemake
A Python-embedded DSL (Snakefiles are Python with added `rule` syntax) directly inspired by GNU Make. Each `rule` declares `input`, `output`, and an action (`shell`/`run`/`script`); rules are connected implicitly by **filename pattern matching** — Snakemake works backward from requested target files, matching output patterns (with `{wildcards}`) to figure out which rule produces them and recursively resolving their inputs. Execution happens in three phases: (1) parse the Snakefile and instantiate rules, (2) build the job DAG by resolving wildcards/inputs bottom-up from targets, (3) schedule and run jobs respecting available resources (cores, memory, custom resources). Rules can call arbitrary Python inline, giving it a lot of scripting flexibility. Widely used in bioinformatics, especially by users already comfortable with Python.

---

## Similarities

| Aspect | Shared trait |
|---|---|
| **Goal** | All express a set of computational steps + data dependencies between them and let an engine determine parallel execution order automatically. |
| **Task abstraction** | Each has a "unit of work" concept (process/task/CommandLineTool/rule) that typically wraps a shell command or script and declares its inputs/outputs. |
| **DAG-based scheduling** | Under the hood, all four ultimately resolve to a directed acyclic graph of jobs — even Nextflow's dataflow model produces an implicit DAG via channel dependencies. |
| **Container/environment support** | All support Docker/Singularity containers and/or Conda environments as the mechanism for reproducible task environments. |
| **HPC/cloud portability** | All are designed to run the same workflow across local machines, HPC schedulers (Slurm, PBS, LSF, SGE), and cloud batch systems, via pluggable backend/executor layers. |
| **Bioinformatics origin** | All four are dominant in genomics/bioinformatics pipelines (though none is limited to that domain) and have strong community pipeline registries (nf-core, Terra/Dockstore, workflowhub, Snakemake Workflow Catalog). |
| **Resumability/caching** | All support re-running a workflow and skipping already-completed steps (Nextflow's `-resume`, Cromwell call-caching, CWL via engine-level caching, Snakemake's timestamp/hash-based rerun triggers). |
| **Language-agnostic task bodies** | The actual work inside a task/process/rule can be written in any language (Python, R, Bash, Perl, etc.) — the workflow language only orchestrates. |
| **Bounded, acyclic structure** | None allow true unbounded loops (`while`) — every fan-out construct (channel iteration, `scatter`, Python `for`, wildcard expansion) is bounded by a known or runtime-resolved collection size, keeping the execution graph acyclic. |

## Differences

| Aspect | Nextflow | WDL | CWL | Snakemake |
|---|---|---|---|---|
| **Paradigm** | Imperative-ish dataflow (Groovy DSL) | Declarative | Declarative, JSON-Schema/data-centric | Declarative rules + embedded imperative Python |
| **Dependency wiring** | Explicit channels connect process outputs to inputs | Explicit workflow block calls tasks and passes variables | Explicit `steps` with typed input/output port connections | **Implicit** — inferred by matching filename patterns between rule outputs and inputs |
| **Execution engine** | Single reference implementation (the Nextflow runtime itself) | Spec-only; multiple independent engines (Cromwell, miniWDL, Toil, Sprocket) | Spec-only; multiple independent engines (cwltool, Toil, Arvados, StreamFlow) | Single reference implementation (the `snakemake` CLI/Python package) |
| **Host language** | Groovy/JVM | Its own DSL (no general-purpose host language) | YAML/JSON (data format, not really a "language") | Python (rules are Python-embedded) |
| **Parallelism trigger** | Data arriving on channels (reactive/streaming) | Static DAG resolved ahead of execution | Static DAG resolved ahead of execution | Backward resolution from requested targets, then static DAG |
| **Typing** | Loosely typed (Groovy dynamic typing, channel-based) | Statically typed (String, Int, File, Array, Struct, etc.) | Strongly typed via JSON Schema-like type system, including complex/custom types | Untyped — everything is essentially Python objects/strings/paths |
| **Modularity model** | DSL2 modules + subworkflows | `import` of other WDL files | `$import`/`$include`, sub-workflows as steps | `include` of other Snakefiles, modules (since Snakemake 6+) |
| **Governance** | Company-backed open source (Seqera) with community RFC process | Community spec (OpenWDL GitHub org), engine-agnostic | Community/nonprofit spec (CWL project), engine-agnostic | Single open-source project (Köster lab), BSD-style community |
| **Learning curve feel** | "New DSL to learn," Groovy syntax quirks | Fairly readable/declarative, close to pseudocode | Verbose YAML/JSON, most "boilerplate-heavy" | Familiar to Python users; Make-like mental model for others |
| **Portability philosophy** | Portable via containers + Nextflow itself running everywhere | Portable via spec — swap engines without changing WDL | Portability is CWL's *primary* design goal — heavy emphasis on strict interchange semantics | Portable via Snakemake itself + conda/containers, less emphasis on interchange across a different engine |
| **Best-known adopters/platforms** | nf-core, Seqera Platform, DNAnexus | Terra, Broad Institute pipelines (GATK best-practices), DNAnexus | Arvados, CWL community reference workflows, various EU bioinformatics infra | Bioinformatics labs using conda/bioconda, Snakemake Workflow Catalog |

## Assumptions Baked Into Each Language

- **Nextflow** assumes: tasks are largely independent, stateless, file/stream-based units best expressed as a reactive flow; that users are comfortable with (or willing to learn) Groovy-flavored syntax; that the single official runtime is an acceptable dependency (no alternate implementations); and that channel semantics (queue vs. value channels, closing/consuming) are intuitive enough once learned.
- **WDL** assumes: a workflow author wants to remain implementation-agnostic and let institutions pick their own engine (Cromwell vs. miniWDL vs. Toil), which in practice has caused **spec-conformance drift** — not all engines support 100% of the same WDL spec version/features. It also assumes static typing catches more errors up front than it costs in verbosity.
- **CWL** assumes: maximal portability and strict interchange are worth the verbosity cost — that a workflow should be expressible as pure structured data (not code) so that any conformant tool can parse, validate, visualize, or execute it without running arbitrary logic. This makes CWL the most "provable"/introspectable, but also the least ergonomic to write by hand.
- **Snakemake** assumes: the filesystem (filenames/paths) is the natural and sufficient interface for expressing dependencies — that inferring the DAG from output-pattern matching is more convenient than declaring edges explicitly. This is powerful for file-centric pipelines but assumes your workflow structure maps cleanly onto naming conventions, which can get fragile as pipelines grow complex (ambiguous wildcard matches, etc.). It also assumes users are fine with Python as the "escape hatch," blurring the workflow language/host language boundary more than the other three.
- **All four** assume the underlying task granularity is at the level of whole processes/commands (coarse-grained parallelism) rather than fine-grained instruction-level parallelism, and that reproducibility is best achieved via containerization/environment pinning rather than language-level guarantees alone.

---

## Control Flow & Language Constructs

A key structural constraint shared by all four: because execution ultimately resolves to a **static or runtime-resolved DAG**, none of them support a true `while` loop or an arbitrary `break`/`continue` statement in the sense of a general-purpose language. "Looping" always means *bounded fan-out over a known or knowable collection*, and "conditionals" always mean *statically or dynamically choosing which subgraph to include*, not branching inside a running task's control flow.

### Nextflow

- **Conditionals:**
  - *Static* (known at parse time, e.g. a `params` flag): plain Groovy `if / else` blocks or ternary expressions choosing which channel-building code runs, and `workflow` blocks can be included/excluded based on parameters.
  - *Dynamic* (depends on channel/runtime data): Groovy `if/else` doesn't work directly on channel contents since channels are asynchronous streams. Instead, use the `.branch{}` operator to split a channel into multiple named sub-channels based on a predicate, or `.filter{}` to drop items — a process downstream only fires if its input channel actually receives data.
  - The `when:` process directive provides a per-task guard: the process block is skipped entirely if the expression evaluates false for a given input.
- **Loops:** No explicit loop construct over pipeline stages. Iteration is implicit — each element emitted on a channel triggers its own instance of a downstream process (equivalent to an implicit `map`/fan-out). Groovy's `for`, `.each`, `.collect()`, etc. can be used *inside* script code (e.g., to build a channel's contents or within a process's script block) but do not loop the dataflow graph itself.
- **Early exit / break:** No `break` statement for the pipeline. Failure handling is controlled via the `errorStrategy` process directive: `terminate` (default — stop launching new tasks, let in-flight ones finish, then fail), `ignore` (log and continue), `retry`/`retryThenIgnore`/`retryThenFinish` (re-attempt failed tasks, with `maxRetries`), and `finish` (a "soft stop" — let all currently running/submitted tasks complete, but launch nothing new). `errorStrategy` can be a closure that inspects the task's exit status to decide dynamically.
- **Abstraction:** DSL2 subworkflows let you factor a chunk of `process` calls into a named, reusable `workflow` block — the closest thing to a function/procedure call.

### WDL

- **Conditionals:** A single-branch `if (expr) { ... }` block wraps one or more `call`s or declarations. **There is no `else` keyword** — the idiomatic workaround is a second `if (!expr) { ... }` block. Any variable declared or produced inside an `if` block becomes an **optional type** (`T?`) outside of it, since the engine can't statically guarantee the block ran. Downstream code typically uses `select_first([...])` or `select_all([...])` to unwrap/merge values from mutually exclusive conditional branches into a single (semi-)required value.
- **Loops:** `scatter (x in some_array) { ... }` is WDL's only iteration construct — a bounded for-each that runs the enclosed `call`(s) once per element of an array, producing array-typed outputs (one output array per output variable). Scatters can be nested, and scatters and conditionals can be combined (e.g., scatter containing an `if`).
- **Early exit / break:** None. There's no way to break out of a `scatter` early or short-circuit a workflow from within WDL itself; failure handling is delegated to the executing engine (e.g., Cromwell generally fails the whole workflow on a task failure, though some engines offer options to continue independent branches) and to task-level `runtime { maxRetries: n }`.
- **Abstraction:** `task` definitions are reusable within a workflow; `import` lets you pull in tasks/workflows/structs from other `.wdl` files, and a `workflow` can itself be called as a sub-workflow from another workflow.

### CWL

- **Conditionals:** Introduced in CWL v1.2 via the `when` field on a workflow `step`. `when` is an expression (typically JS, via `InlineJavascriptRequirement`) evaluated against the step's inputs; if it evaluates falsy, the step is **skipped**, and all of its output parameters resolve to `null` rather than running. Because a skipped step still exists in the graph (just with null outputs), downstream steps that consume its output must handle nulls — CWL provides `pickValue` (`the_only_non_null`, `first_non_null`, `all_non_null`) on a downstream input to select a real value among several conditionally-produced candidates, functioning much like an if/else merge.
- **Loops:** `scatter` on a step, with a `scatterMethod` controlling how multiple scattered inputs combine: `dotproduct` (parallel zip — arrays must be equal length), `nested_crossproduct` (all combinations, nested output arrays), or `flat_crossproduct` (all combinations, flattened). This is CWL's only iteration mechanism, bounded by the length of the input array(s) at the time the step is reached.
- **Early exit / break:** None built into the spec — CWL is pure declarative data with no control-flow escape hatch. A step failure generally fails the enclosing workflow, though `when` + null-propagation can be used to *design around* certain failure/skip paths, and some engines (e.g., via `--on-error continue`) let independent branches keep running after one branch fails.
- **Abstraction:** `SubworkflowFeatureRequirement` allows a `Workflow` to be nested/used as a step inside another `Workflow`, giving a form of composition; `$import`/`$include` pull in shared fragments (tool or type definitions) across files.

### Snakemake

- **Conditionals:** Because a Snakefile is literally Python plus `rule` syntax, you get **full native Python `if/elif/else`** at parse time — to decide which rules get defined, what a rule's `input:`/`output:`/`params:` should resolve to, or to select between code paths inside a `run:` block. Input functions (plain Python functions passed to `input:`) can branch on wildcard values or on config/checkpoint state to select different upstream files.
- **Loops:** Full native Python `for` loops, list/dict comprehensions, and the `expand()` helper function (which programmatically generates a list of filenames by substituting wildcard values) — used at parse time to generate many similar rule instances or to build large input lists. This is compile-time/metaprogramming-style looping, more powerful than the bounded `scatter`/channel constructs in the other three.
- **Dynamic control flow:** `checkpoints` (Snakemake ≥5.4) are the standout feature among all four languages — a checkpoint is a rule whose output isn't fully known until it actually runs (e.g., an unknown number of output files from a clustering step). After a checkpoint job completes, Snakemake **re-evaluates the DAG**: input functions that reference `checkpoints.<name>.get(...)` are re-run with access to the checkpoint's actual output, and can branch on the contents of those files to decide what the rest of the DAG looks like. This is the closest analogue among the four to a genuine runtime-conditional/dynamically-unrolled loop.
- **Early exit / break:** No `break` mid-DAG, but you can `raise` an exception inside a `run:` block or an input function to halt with a custom error. At the whole-workflow level, `onstart`/`onsuccess`/`onerror` handlers let you run arbitrary Python on those lifecycle events, and `--keep-going` vs. the default fail-fast behavior controls whether independent branches continue after one job fails. `ruleorder` resolves ambiguity when multiple rules could produce the same output (not a loop/conditional per se, but a control-flow-adjacent disambiguation mechanism).
- **Abstraction:** `include:` pulls in other Snakefiles; since Snakemake 6, `module` blocks allow importing and selectively overriding rules from another Snakemake workflow, giving stronger reuse than plain `include`.

### Summary Table: Control Flow Constructs

| Construct | Nextflow | WDL | CWL | Snakemake |
|---|---|---|---|---|
| **If/else** | Groovy `if/else` (static); `.branch{}`/`.filter{}` (dynamic, on channels); `when:` directive (per-task guard) | `if (expr) {}` only — no `else`; paired `if`/`if (!expr)` + `select_first()` idiom | `when:` field on a step (v1.2+); skipped step outputs `null`; merged via `pickValue` | Native Python `if/elif/else` at parse time; branching input functions |
| **Loop / fan-out** | Implicit — one process instance per channel element; Groovy `for`/`.each` inside scripts only | `scatter (x in array) {}` — bounded for-each, nestable | `scatter` on a step, with `scatterMethod` (`dotproduct`, `nested_crossproduct`, `flat_crossproduct`) | Native Python `for`/comprehensions + `expand()` at parse time (compile-time looping) |
| **Dynamic/runtime-conditioned DAG** | Channels react to data as it arrives (reactive), but graph shape is fixed at parse time | Not supported — DAG is static once inputs are typed/resolved | Not supported — DAG is static; `when` only prunes, doesn't reshape | `checkpoints` — DAG is *re-planned* after a checkpoint rule runs, based on its actual output |
| **Early exit / break** | No `break`; `errorStrategy` (`terminate`/`ignore`/`retry`/`finish`) governs failure response | None; engine-level failure propagation only | None; engine-level failure propagation only, `pickValue`/`when` for designed-around paths | No `break`; `raise` in `run:`/input functions, `onerror` handler, `--keep-going` flag |
| **Reusable subroutines** | DSL2 modules + subworkflows | `task`/`workflow` reuse via `import` | Nested `Workflow` as a step (`SubworkflowFeatureRequirement`), `$import`/`$include` | `include:` and `module` (Snakemake 6+) |

### Takeaway

Snakemake is the outlier in expressiveness because it *is* Python — you get arbitrary imperative control flow at workflow-construction time, plus `checkpoints` for genuine runtime-conditioned DAG reshaping, which none of the other three can do natively. Nextflow sits in the middle: Groovy gives it real conditionals and the `errorStrategy` mechanism gives it more nuanced failure handling than WDL or CWL, but the dataflow model means "looping" is always implicit in channel fan-out. WDL and CWL are the most constrained and the most "pure declarative DAG" — both support only a bounded `scatter`-style loop and a limited conditional (`if`-only in WDL, `when` in CWL), by design, since that constraint is exactly what makes them easy to interchange between execution engines.

---

## Sources

**Nextflow**
- [A DSL for parallel and scalable computational pipelines | Nextflow](https://www.nextflow.io/)
- [Nextflow | Seqera Docs](https://www.nextflow.io/docs/latest/)
- [Syntax — Nextflow documentation](https://www.nextflow.io/docs/latest/reference/syntax.html)
- [Process reference — Nextflow documentation](https://www.nextflow.io/docs/latest/reference/process.html)
- [Nextflow DSL 2 is here! | Seqera](https://seqera.io/blog/dsl2-is-here/)
- [Conditional process execution (static) - Nextflow Patterns](https://nextflow-io.github.io/patterns/conditional-process/)
- [Conditional process execution (dynamic) - Nextflow Patterns](https://nextflow-io.github.io/patterns/conditional-process-dynamic/)
- [Ignore failing process - Nextflow Patterns](https://nextflow-io.github.io/patterns/ignore-failing-process/)
- [Error Strategies for Nextflow | DNAnexus Academy Documentation](https://academy.dnanexus.com/buildingworkflows/nf/errorstrategies)
- [Troubleshooting - training.nextflow.io](https://training.nextflow.io/2.0/basic_training/debugging/)

**WDL (Workflow Description Language)**
- [OpenWDL](https://openwdl.org/)
- [GitHub - openwdl/wdl: Specification for the Workflow Description Language (WDL)](https://github.com/openwdl/wdl)
- [wdl/SPEC.md at wdl-1.2 · openwdl/wdl](https://github.com/openwdl/wdl/blob/wdl-1.2/SPEC.md)
- [Workflow Description Language (WDL) Documentation](http://docs.openwdl.org/)
- [Workflows | WDL Documentation](https://docs.openwdl.org/language-guide/workflows.html)
- [Which workflow execution engines support WDL? What is Cromwell? – Terra Support](https://support.terra.bio/hc/en-us/articles/360037128472-Which-workflow-execution-engines-support-WDL-What-is-Cromwell)
- [WDL Execution Engines - Fred Hutch](https://sciwiki.fredhutch.org/datademos/wdl_execution_engines/)
- [WDL | Doc #6715 | Conditionals (if/else)](https://software.broadinstitute.org/wdl/documentation/article?id=6715)
- [Conditionals (if/else) – Terra Support](https://support.terra.bio/hc/en-us/articles/360037128512-Conditionals-if-else)
- [WDL workflow with optional input — GATK-Forum](https://gatkforums.broadinstitute.org/wdl/discussion/10775/wdl-workflow-with-optional-input)

**CWL (Common Workflow Language)**
- [Home | Common Workflow Language (CWL)](https://www.commonwl.org/)
- [Specification | Common Workflow Language (CWL)](https://www.commonwl.org/specification/)
- [Common Workflow Language (CWL) Workflow Description, v1.2.1](https://www.commonwl.org/v1.2/Workflow.html)
- [Common Workflow Language (CWL) Workflow Description, v1.1](https://www.commonwl.org/v1.1/Workflow.html)
- [2.10. Workflows — Common Workflow Language User Guide](https://www.commonwl.org/user_guide/topics/workflows.html)
- [CWL v1.2 conditional execution - The CGC Knowledge Center](https://docs.cancergenomicscloud.org/docs/cwl-v12-conditional-execution)
- [CWL v1.2 conditional execution - Cavatica Docs](https://docs.cavatica.org/docs/cwl-v12-conditional-execution)
- [cwl-patterns: control flow patterns](https://github.com/common-workflow-library/cwl-patterns/blob/master/workflow_patterns_initiative/control/README.md)
- [Common pitfalls and limitations of CWL — BioExcel](http://docs.bioexcel.eu/cwl-best-practice-guide/limitations.html)

**Snakemake**
- [Snakefiles and Rules — Snakemake documentation](https://snakemake.readthedocs.io/en/stable/snakefiles/rules.html)
- [Rules — Snakemake documentation](https://snakemake.readthedocs.io/en/v5.22.1/snakefiles/rules.html)
- [Advanced: Decorating the example workflow | Snakemake documentation](https://snakemake.readthedocs.io/en/stable/tutorial/advanced.html)
- [Snakemake Tutorial](https://snakemake.bitbucket.io/snakemake-tutorial.html)
- [How do users design scientific workflows? The Case of Snakemake (arXiv)](https://arxiv.org/pdf/2309.14097)
