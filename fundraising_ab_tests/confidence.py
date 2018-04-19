from process.globals import get_config
from stats.stats_abba import Experiment


def add_confidence(results, name_column, successes_column):
    confidence = get_confidence(results, name_column, successes_column)
    if confidence:
        for i, levels in enumerate(confidence):
            results[i].results.update({
                'p-value': levels.two_tailed_p_value,
                'improvement': levels.relative_improvement.value * 100,
            })
    results[0].results.update({
        'confidencelink': get_confidence_link(results, name_column, successes_column)
    })


def get_confidence(results, name_column=None, successes_column=None, trials=None):
    num_test_cases = len(results)

    if not num_test_cases:
        return

    results = sorted(results, key=lambda result: result.results[successes_column])
    baseline_successes = None
    for result in results:
        if result.results[successes_column]:
            baseline_successes = result.results[successes_column]
            break

    if not baseline_successes:
        return

    config = get_config()
    experiment = Experiment(
        num_trials=config.fudge_trials,
        baseline_num_successes=baseline_successes,
        baseline_num_trials=config.fudge_trials,
        confidence_level=config.confidence_level
    )
    # useMultipleTestCorrection=true

    cases = []
    for result in results:
        # name = result.results[name_column]
        successes = result.results[successes_column]
        if hasattr(trials, 'encode'):
            trials = result.results[trials]
        else:
            trials = config.fudge_trials
        calculated = experiment.get_results(num_successes=successes, num_trials=trials)
        cases.append(calculated)

    return cases


def get_confidence_link(results, name_column, successes_column):
    cases = []
    config = get_config()
    for result in results:
        # skip empty results, usually these will be "blank" banners
        if not result.results[successes_column]:
            continue
        name = result.results[name_column]
        successes = result.results[successes_column]
        cases.append("%s=%s,%s" % (name, successes, config.fudge_trials))

    return "http://www.thumbtack.com/labs/abba/#%(cases)s&abba:intervalConfidenceLevel=%(confidence)s&abba:useMultipleTestCorrection=true" % {
        'cases': "&".join(cases),
        'confidence': config.confidence_level,
    }
