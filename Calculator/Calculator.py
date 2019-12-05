from Calculator.statistics.Confidence_interval import Confidence_interval
from Calculator.statistics.Mean import Mean
from Calculator.statistics.Median import Median
from Calculator.statistics.Mode import Mode
from Calculator.statistics.P_value import P_value
from Calculator.statistics.Pop_correlation_coefficient import Pop_correlation_coefficient
from Calculator.statistics.Proportion import Proportion
from Calculator.statistics.Sample_mean import Sample_mean
from Calculator.statistics.Sample_sd import Sample_sd
from Calculator.statistics.Sd import Sd
from Calculator.statistics.Standardised_score import Standardised_score
from Calculator.statistics.Variance import Variance
from Calculator.statistics.Variance_popu_proportion import Variance_popu_proportion
from Calculator.statistics.Variance_sample_proportion import Variance_sample_proportion
from Calculator.statistics.Z_score import Z_score
from  CsvReader.Write_answer import write_answer
from random import sample
import math

class Calculator:
    def __init__(self):
        pass
    def mean(self,my_population):
        return round(Mean(my_population),2)

    def median(self, my_population):
        return Median(my_population)


    def mode(self, my_population):
        return Mode(my_population)

    def sd(self, my_population):
        return round(Sd(my_population),2)


    def variance_popu_proportion(self, my_population):
        p, proportion_success = Proportion(my_population)
        write_answer("answer_variance_popu_proportion.csv", round(Variance(proportion_success), 2))
        return round(Variance_popu_proportion(my_population),2)

    def z_score(self,my_population):
        new_z_score1 =  Z_score(my_population)
        new_z_score = list()
        for x in new_z_score1:
            new_z_score.append(round(x,2)) #creating rounded zscore upto 2 decimal points
        return new_z_score

    def standardised_score(self,my_population):
        new_standardised_score1 = Standardised_score(my_population)
        new_standardised_score = list()
        for x in new_standardised_score1:
            new_standardised_score.append(round(x, 2))  # creating rounded zscore upto 2 decimal points
        return new_standardised_score

    def pop_correlation_coefficient(self,my_population):
        my_population2 = list(map(lambda x: x + 5, my_population))
        return round(Pop_correlation_coefficient(my_population, my_population2),2)

    def confidence_interval(self,my_population):
        confidence_level = 0.95
        return round(Confidence_interval(my_population,confidence_level),2)

    def variance(self,my_population):
        return round(Variance(my_population),2)

    def p_value(self,my_population):
        confidence_level = 0.95
        return round(P_value(my_population, confidence_level), 2)
    def proportion(self,my_population):
        p,t = Proportion(my_population)
        return round(p,2)

    def sample_mean(self,my_population):
        sample_list = sample(my_population,math.floor(len(my_population)/2))
        write_answer("answer_sample_mean.csv",Mean(sample_list))
        return Sample_mean(sample_list)

    def sample_sd(self,my_population):
        sample_list = sample(my_population, math.floor(len(my_population) / 2))
        write_answer("answer_sample_sd.csv", round(Sd(sample_list),2))
        return round(Sample_sd(sample_list),2)

    def variance_sample_proportion(self,my_population):
        sample_list = sample(my_population, math.floor(len(my_population) / 2))
        p,proportion_success = Proportion(sample_list)
        write_answer("answer_variance_sample_proportion.csv", round(Variance(proportion_success),2))
        return round(Variance_sample_proportion(sample_list),2)


