import {
  JupyterFrontEnd,
  JupyterFrontEndPlugin
} from '@jupyterlab/application';

import { ICompletionProviderManager } from '@jupyterlab/completer';
import { INotebookTracker } from '@jupyterlab/notebook';
import { ISettingRegistry } from '@jupyterlab/settingregistry';

import { PostgresCompletionProvider } from './provider';

/**
 * Plugin ID constant.
 */
const PLUGIN_ID = 'jl_db_comp:plugin';

/**
 * Initialization data for the jl_db_comp extension.
 *
 * This plugin provides PostgreSQL table and column name completions
 * in JupyterLab notebooks and editors when typing SQL queries.
 * It integrates with jupysql's connections.ini for database configuration.
 */
const plugin: JupyterFrontEndPlugin<void> = {
  id: PLUGIN_ID,
  description:
    'A JupyterLab extension to complete db queries in jupyterlab notebooks',
  autoStart: true,
  requires: [ICompletionProviderManager],
  optional: [ISettingRegistry, INotebookTracker],
  activate: (
    _app: JupyterFrontEnd,
    completionManager: ICompletionProviderManager,
    settingRegistry: ISettingRegistry | null,
    notebookTracker: INotebookTracker | null
  ) => {
    let provider: PostgresCompletionProvider;

    if (settingRegistry) {
      settingRegistry
        .load(PLUGIN_ID)
        .then(settings => {
          provider = new PostgresCompletionProvider(settings, notebookTracker);
          completionManager.registerProvider(provider);
          console.log('JupyterLab extension jl_db_comp is activated!');
        })
        .catch(reason => {
          console.error('Failed to load settings for jl_db_comp:', reason);
          provider = new PostgresCompletionProvider(null, notebookTracker);
          completionManager.registerProvider(provider);
          console.log('JupyterLab extension jl_db_comp is activated!');
        });
    } else {
      provider = new PostgresCompletionProvider(null, notebookTracker);
      completionManager.registerProvider(provider);
      console.log('JupyterLab extension jl_db_comp is activated!');
    }
  }
};

export default plugin;
