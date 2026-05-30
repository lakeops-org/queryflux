/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React, {type ReactNode} from 'react';
import clsx from 'clsx';
import {ThemeClassNames} from '@docusaurus/theme-common';
import useBaseUrl from '@docusaurus/useBaseUrl';
import LinkItem from '@theme/Footer/LinkItem';
import type {Props} from '@theme/Footer/Links/MultiColumn';

type ColumnType = Props['columns'][number];
type ColumnItemType = ColumnType['items'][number];

function isLakeOpsBrandColumn(column: ColumnType): boolean {
  return Boolean(column.className?.includes('footer__col--lakeops'));
}

function LakeOpsFooterColumn(): ReactNode {
  const logoUrl = useBaseUrl('/img/lakeops-logo.png');

  return (
    <div
      className={clsx(
        ThemeClassNames.layout.footer.column,
        'col',
        'footer__col',
        'footer__col--lakeops',
      )}>
      <a
        href="https://lakeops.dev/"
        className="footer__lakeops-brand"
        aria-label="LakeOps — visit lakeops.dev"
        target="_blank"
        rel="noopener noreferrer">
        <img
          className="footer__lakeops-logo"
          src={logoUrl}
          alt="LakeOps"
          width={180}
          height={180}
          loading="lazy"
          decoding="async"
        />
      </a>
    </div>
  );
}

function ColumnLinkItem({item}: {item: ColumnItemType}) {
  return item.html ? (
    <li
      className={clsx('footer__item', item.className)}
      // Developer provided the HTML, so assume it's safe.
      // eslint-disable-next-line react/no-danger
      dangerouslySetInnerHTML={{__html: item.html}}
    />
  ) : (
    <li key={item.href ?? item.to} className="footer__item">
      <LinkItem item={item} />
    </li>
  );
}

function Column({column}: {column: ColumnType}) {
  if (isLakeOpsBrandColumn(column)) {
    return <LakeOpsFooterColumn />;
  }

  return (
    <div
      className={clsx(
        ThemeClassNames.layout.footer.column,
        'col footer__col',
        column.className,
      )}>
      {column.title != null && column.title !== '' ? (
        <div className="footer__title">{column.title}</div>
      ) : null}
      <ul className="footer__items clean-list">
        {column.items.map((item, i) => (
          <ColumnLinkItem key={i} item={item} />
        ))}
      </ul>
    </div>
  );
}

export default function FooterLinksMultiColumn({columns}: Props): ReactNode {
  return (
    <div className="row footer__links">
      {columns.map((column, i) => (
        <Column key={i} column={column} />
      ))}
    </div>
  );
}
