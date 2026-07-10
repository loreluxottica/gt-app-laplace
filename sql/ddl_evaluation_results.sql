-- evaluation_results — written by the Ground Truth App.
-- One row per (file, annotation event): human ground truth vs model boundaries.
-- Run once before first use. Catalog/schema must match app.yaml.

CREATE TABLE IF NOT EXISTS `sbx-logistics`.`multidocument-us`.`evaluation_results` (
    filename            STRING,
    folder_id           STRING,
    total_pages         INT,

    -- Human ground truth
    gt_starts           ARRAY<INT>,
    gt_n_documents      INT,
    gt_is_multidoc      BOOLEAN,

    -- Model prediction (from split_results); NULL if file not yet split
    model_starts        ARRAY<INT>,
    model_n_documents   INT,
    model_used          STRING,

    -- Strict comparison (full sets, page 1 included)
    exact_match         BOOLEAN,

    -- Exact boundary metrics (page 1 excluded — it is always a trivial boundary)
    n_true_positive     INT,
    n_false_positive    INT,
    n_false_negative    INT,
    precision           DOUBLE,
    recall              DOUBLE,
    f1                  DOUBLE,

    -- Tolerant metrics (a predicted boundary within +/-1 page counts as a match)
    n_offby1            INT,
    precision_tol       DOUBLE,
    recall_tol          DOUBLE,
    f1_tol              DOUBLE,

    -- Multi-document classification correctness
    multidoc_correct    BOOLEAN,

    annotator           STRING,
    annotated_at        TIMESTAMP
)
USING DELTA
COMMENT 'Ground-truth vs model boundary evaluation, produced by the GT annotation app.';
