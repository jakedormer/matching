import recordlinkage
import pandas



data = pandas.read_csv('/home/jake/Documents/matching/functions/comparison_index.csv')

matches = data[0:316]
matches = matches[['sku_1', 'sku_2']]
matches = pandas.MultiIndex.from_frame(matches)

data = pandas.read_csv('/home/jake/Documents/matching/functions/comparison_index.csv', index_col=['sku_1', 'sku_2'])

golden_pairs = data.sample(frac=1)
golden_pairs = golden_pairs[0:5000]
golden_matches_index = golden_pairs.index & matches
print(golden_matches_index)


data_2 = pandas.read_csv('/home/jake/Documents/matching/functions/comparison_index.csv', index_col=['sku_1', 'sku_2'])


logreg = recordlinkage.LogisticRegressionClassifier()

logreg.fit(golden_pairs, golden_matches_index)
print ("Intercept: ", logreg.intercept)
print ("Coefficients: ", logreg.coefficients)

result_logreg = logreg.predict(data_2)

print(len(result_logreg))
print(result_logreg)

print(recordlinkage.confusion_matrix(matches, result_logreg, len(data_2)))

print(recordlinkage.fscore(matches, result_logreg))

coefficients = [2, -0.08400654, -0.41432631, -0.12138752, -0.31617086, -0.42389099, -0.33185166, 0.02173983, 0]
predicter = recordlinkage.LogisticRegressionClassifier(coefficients=coefficients, intercept=-5.379865263857996)

y = predicter.predict(data_2)
