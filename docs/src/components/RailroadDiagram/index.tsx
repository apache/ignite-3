import React, { useEffect, useRef } from 'react';
import * as rr from './railroad.js';
import 'railroad-diagrams/railroad-diagrams.css';

interface RailroadDiagramProps {
  children: string;
}

export default function RailroadDiagram({ children }: RailroadDiagramProps): JSX.Element {
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (containerRef.current) {
      try {
        // The custom railroad library supports href, title, and cls options on NonTerminal and Terminal
        // Create function wrappers that can be called without 'new'
        const Diagram = (...args: any[]) => new rr.Diagram(...args);
        const Terminal = (...args: any[]) => new rr.Terminal(...args);
        const NonTerminal = (...args: any[]) => new rr.NonTerminal(...args);
        const Optional = (...args: any[]) => new rr.Optional(...args);
        const Sequence = (...args: any[]) => new rr.Sequence(...args);
        const Choice = (...args: any[]) => new rr.Choice(...args);
        const Skip = (...args: any[]) => new rr.Skip(...args);
        const Comment = (...args: any[]) => new rr.Comment(...args);
        const OneOrMore = (...args: any[]) => new rr.OneOrMore(...args);
        const ZeroOrMore = (...args: any[]) => new rr.ZeroOrMore(...args);

        // Strip Start() and End() calls since Diagram() adds them automatically
        // Also replace ComplexDiagram with Diagram
        const cleanedCode = children
          .replace(/ComplexDiagram\(/g, 'Diagram(')
          .replace(/Start\([^)]*\),?\s*/g, '')
          .replace(/,?\s*End\([^)]*\)/g, '')
          .trim(); // CRITICAL: Remove leading/trailing whitespace to avoid 'return\nDiagram()' syntax error

        // Create function that executes the diagram code
        try {
          const diagramFunc = new Function(
            'Diagram', 'Terminal', 'NonTerminal', 'Optional', 'Sequence', 'Choice', 'Skip', 'Comment', 'OneOrMore', 'ZeroOrMore',
            `'use strict'; return ${cleanedCode};`
          );

          // Execute and get the diagram object
          const diagram = diagramFunc(Diagram, Terminal, NonTerminal, Optional, Sequence, Choice, Skip, Comment, OneOrMore, ZeroOrMore);

          if (!diagram || typeof diagram.toSVG !== 'function') {
            throw new Error('Diagram function did not return a valid diagram object');
          }

          // Convert to SVG and inject into container
          containerRef.current.innerHTML = '';
          const svgElement = diagram.toSVG();

          // Process links to convert relative paths to Docusaurus routes
          const links = svgElement.querySelectorAll('a[*|href]');
          links.forEach((link) => {
            const href = link.getAttributeNS('http://www.w3.org/1999/xlink', 'href');
            if (href && href.startsWith('./')) {
              // Convert ./file#anchor to ../current-dir/file#anchor format
              const converted = href.replace(/^\.\//, '../sql-reference/');
              link.setAttributeNS('http://www.w3.org/1999/xlink', 'href', converted);
            }
          });

          containerRef.current.appendChild(svgElement);
        } catch (innerError) {
          console.error('Error in diagram generation:', innerError);
          throw innerError;
        }
      } catch (error) {
        console.error('Error rendering railroad diagram:', error);
        containerRef.current.innerHTML = '<p style="color: red;">Error rendering diagram. Check console for details.</p>';
      }
    }
  }, [children]);

  return <div ref={containerRef} className="railroad-diagram-container" />;
}
