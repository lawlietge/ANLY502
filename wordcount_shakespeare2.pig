-- wordcount_shakespeare2.pig
--
-- Find the top-20 words in Shakespeare and produce a sorted output.
--
-- Clear the output directory location
--
rmf sorted_words1

--
-- Run the script

shakespeare = LOAD 's3://gu-anly502/ps04/Shakespeare.txt' as (line:chararray);
words = foreach shakespware generate LOWER(flatten(TOKENIZE(line))) as word;
grouped = GROUP words by word;
wordcount = FOREACH grouped GENERATE group, COUNT(words)
sorted_words = ORDER wordcount BY $1 DESC;
sorted_words20 = limit sorted_words 20; 



-- YOUR CODE GOES HERE
-- PUT YOUR RESULTS IN sorted_words20

STORE sorted_words20 INTO 'sorted_words2' USING PigStorage();
 
-- Get the results
--
fs -getmerge sorted_words2 wordcount_shakespeare2.txt