CREATE TABLE text_int_pk(
    text_pk TEXT,
    int_pk INT,
    PRIMARY KEY (text_pk, int_pk)
);

INSERT INTO text_int_pk (text_pk, int_pk) VALUES
  ('1', 1),
  ('2', 2),
  ('3', 3),
  ('4', 4),
  ('5', 5),
  ('6', 6),
  ('7', 7),
  ('8', 8),
  ('9', 9),
  ('10', 10);
