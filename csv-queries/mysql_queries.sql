SELECT Species, COUNT(*) AS Count, AVG(Seq_length) AS avg_SeqLength
FROM microorganism
GROUP BY Species
HAVING COUNT(*) > 1;